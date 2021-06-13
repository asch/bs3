// Copyright (C) 2021 Vojtech Aschenbrenner <v@asch.cz>

// Package objproxy is a proxy for ObjectUploadDownloaderAt which performs
// prioritization of various requests.
package objproxy

import (
	"time"
)

// Interface for s3 backend storage. Anything implementing this interface can
// be used as a storage backend.
type ObjectUploadDownloaderAt interface {
	// Uploads data in buf under the key identifier.
	Upload(key int64, buf []byte) error

	// Downloads data into buf starting from offset in the object
	// identified by key. The length of buf is the legth of requested data.
	DownloadAt(key int64, buf []byte, offset int64) error

	// Returns size in bytes of object identified by key. Needed only for
	// garbage collection and extent map recovery. Otherwise can have empty
	// implementation.
	GetObjectSize(key int64) (int64, error)

	// Deletes object identified by key and all successive objects. Needed
	// only for extent map restoration. Otherwise can have empty
	// implementation.
	DeleteKeyAndSuccessors(key int64) error
}

// Proxy for the backend storage which prioritizes requests. Requests coming to
// the priority channels are handled first. Like this requests from low
// priority operations like garbage collection do not slow down normal
// operation.
type ObjectProxy struct {
	Instance ObjectUploadDownloaderAt

	// Number of go routines to spawn for handling upload requests and
	// download requests.
	uploaders   int
	downloaders int

	// Timeout after which low priority request can be served.
	idleTimeout time.Duration

	// Internal channels.
	uploads       chan request
	downloads     chan request
	uploadsPrio   chan request
	downloadsPrio chan request
}

// Request is internal structure for wrapping the communication into channels.
type request struct {
	key    int64
	data   []byte
	offset int64
	done   chan error
}

// Return new instance of the proxy which can be directly used. It immediately
// spawns go routines for upload and download workers.
func New(storeInstance ObjectUploadDownloaderAt, uploaders, downloaders int,
	idleTimeout time.Duration) ObjectProxy {

	uploads := make(chan request)
	downloads := make(chan request)
	uploadsPrio := make(chan request)
	downloadsPrio := make(chan request)

	s := ObjectProxy{
		Instance:      storeInstance,
		uploaders:     uploaders,
		downloaders:   downloaders,
		idleTimeout:   idleTimeout,
		uploads:       uploads,
		downloads:     downloads,
		uploadsPrio:   uploadsPrio,
		downloadsPrio: downloadsPrio,
	}

	for i := 0; i < s.uploaders; i++ {
		go s.uploadWorker()
	}

	for i := 0; i < s.downloaders; i++ {
		go s.downloadWorker()
	}

	return s
}

// Proxy function for uploading the object with key. It selects the right
// channel according to prio and waits for reply.
func (p *ObjectProxy) Upload(key int64, body []byte, prio bool) error {
	c := p.uploads
	if prio {
		c = p.uploadsPrio
	}

	done := make(chan error)
	c <- request{key: key, data: body, done: done}
	return <-done
}

// Proxy function for downloading the object with key. It selects the right
// channel according to prio and waits for reply.
func (p *ObjectProxy) Download(key int64, chunk []byte, offset int64, prio bool) error {
	c := p.downloads
	if prio {
		c = p.downloadsPrio
	}

	done := make(chan error)
	c <- request{key, chunk, offset, done}
	return <-done
}

// Generic function for prioritization used by both, uploader and downloader workers.
func (p *ObjectProxy) receiveRequest(prio chan request, normal chan request) request {
	var r request

	select {
	case r = <-prio:
	//case <-time.NewTicker(p.idleTimeout).C:
	default:
		select {
		case r = <-prio:
		case r = <-normal:
		}
	}

	return r
}

// Upload worker just calls Upload() on the instance provided in New().
func (p *ObjectProxy) uploadWorker() {
	for {
		r := p.receiveRequest(p.uploadsPrio, p.uploads)
		err := p.Instance.Upload(r.key, r.data)
		r.done <- err
	}
}

// Upload worker just calls Download() on the instance provided in New().
func (p *ObjectProxy) downloadWorker() {
	for {
		r := p.receiveRequest(p.downloadsPrio, p.downloads)
		err := p.Instance.DownloadAt(r.key, r.data, r.offset)
		r.done <- err
	}
}
