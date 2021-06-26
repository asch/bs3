// Copyright (C) 2021 Vojtech Aschenbrenner <v@asch.cz>

// Package s3 implements wrapping functions to satisfy ObjectUploadDownloaderAt
// interface. It uses aws api v1.
package s3

import (
	"bytes"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"golang.org/x/net/http2"
)

const (
	// Format string for the object key. There should be no need to set
	// this differently, hence the constant. If you want to change it, keep
	// in mind that we rely on the continuous space of keys for prefix
	// consistecy as well as in the GC process.
	//
	// Furthermore we split the key into halves and use the lower half of
	// bits as s3 prefix and upper half for the object key. This is to
	// prevent s3 rate limiting which is applied to objects with the same
	// prefix.
	keyFmt = "%08x/%08x"
)

// Implementation of ObjectUploadDownloaderAt using AWS S3 as a backend.
// Parameters of http connection are carefully tuned for the best performance
// in the AWS environment.
type S3 struct {
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	client     *s3.S3
	bucket     string
}

// Options to use in New() function due to high number of parameters. There is
// lower chance of ordering mistake with named parameters.
type Options struct {
	Remote    string
	Region    string
	Bucket    string
	AccessKey string
	SecretKey string
	PartSize  int64
}

// Helper struct used for tuning the http connection.
type httpClientSettings struct {
	connect          time.Duration
	connKeepAlive    time.Duration
	expectContinue   time.Duration
	idleConn         time.Duration
	maxAllIdleConns  int
	maxHostIdleConns int
	responseHeader   time.Duration
	tlsHandshake     time.Duration
}

// Returns http client with configured parameters and added https2 support.
func newHTTPClientWithSettings(httpSettings httpClientSettings) *http.Client {
	tr := &http.Transport{
		ResponseHeaderTimeout: httpSettings.responseHeader,
		Proxy:                 http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			KeepAlive: httpSettings.connKeepAlive,
			DualStack: true,
			Timeout:   httpSettings.connect,
		}).DialContext,
		MaxIdleConns:          httpSettings.maxAllIdleConns,
		IdleConnTimeout:       httpSettings.idleConn,
		TLSHandshakeTimeout:   httpSettings.tlsHandshake,
		MaxIdleConnsPerHost:   httpSettings.maxHostIdleConns,
		ExpectContinueTimeout: httpSettings.expectContinue,
	}

	http2.ConfigureTransport(tr)

	return &http.Client{
		Transport: tr,
	}
}

// Upload function implemented through s3 api.
func (s *S3) Upload(key int64, buf []byte) error {
	_, err := s.uploader.Upload(&s3manager.UploadInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(encode(key)),
		Body:   bytes.NewReader(buf),
	})

	return err
}

// GetObjectSize function implemented through s3 api.
func (s *S3) GetObjectSize(key int64) (int64, error) {
	head, err := s.client.HeadObject(&s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(encode(key)),
	})

	var size int64
	if err == nil {
		size = *head.ContentLength
	}

	return size, err
}

// DownloadAt function implemented through s3 api.
func (s *S3) DownloadAt(key int64, buf []byte, offset int64) error {
	to := offset + int64(len(buf)) - 1
	rng := fmt.Sprintf("bytes=%d-%d", offset, to)
	b := aws.NewWriteAtBuffer(buf)

	_, err := s.downloader.Download(b, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(encode(key)),
		Range:  &rng,
	})

	return err
}

// Delete function implemented through s3 api.
func (s *S3) Delete(key int64) error {
	_, err := s.client.DeleteObject(&s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(encode(key)),
	})

	return err
}

func New(o Options) (*S3, error) {
	s := new(S3)
	s.bucket = o.Bucket

	// For the best possible performance (throughput close to 10GB/s) it
	// should be tuned according to the object backend.
	// Following settings are recommended by AWS for usage in their
	// network.
	httpClient := newHTTPClientWithSettings(httpClientSettings{
		connect:          5 * time.Second,
		expectContinue:   1 * time.Second,
		idleConn:         90 * time.Second,
		connKeepAlive:    30 * time.Second,
		maxAllIdleConns:  100,
		maxHostIdleConns: 10,
		responseHeader:   5 * time.Second,
		tlsHandshake:     5 * time.Second,
	})

	sess, err := session.NewSession(&aws.Config{
		Endpoint:                      aws.String(o.Remote),
		Region:                        aws.String(o.Region),
		Credentials:                   credentials.NewStaticCredentials(o.AccessKey, o.SecretKey, ""),
		S3ForcePathStyle:              aws.Bool(true),
		S3DisableContentMD5Validation: aws.Bool(true),
		HTTPClient:                    httpClient,
	})

	if err != nil {
		return nil, err
	}

	s.client = s3.New(sess)
	s.uploader = s3manager.NewUploader(sess)
	s.downloader = s3manager.NewDownloader(sess)

	// Limiting the concurency of s3 library. We do not benefit from
	// multipart uploads/downloads because we have small objects. The only
	// exception is downloading/uploading the extent map during initial
	// recover or final map upload. This should be tuned if your map is
	// huge (= huge device) and you have fast network and don't want to
	// wait.
	s.uploader.Concurrency = 1
	s3manager.WithUploaderRequestOptions(request.Option(func(r *request.Request) {
		r.HTTPRequest.Header.Add("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
	}))(s.uploader)
	s.downloader.Concurrency = 1

	err = s.makeBucketExist()

	return s, err
}

// Check whether bucket exist and if not, create it and wait until it appears.
func (s *S3) makeBucketExist() error {
	_, err := s.client.HeadBucket(&s3.HeadBucketInput{Bucket: aws.String(s.bucket)})

	if err != nil {
		_, err = s.client.CreateBucket(&s3.CreateBucketInput{
			Bucket: aws.String(s.bucket)})

		if err == nil {
			err = s.client.WaitUntilBucketExists(&s3.HeadBucketInput{
				Bucket: aws.String(s.bucket)})
		}
	}

	return err
}

// Delete object with key and all objects with higher keys.
func (s *S3) DeleteKeyAndSuccessors(fromKey int64) error {
	err := s.client.ListObjectsV2Pages(&s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
	}, func(page *s3.ListObjectsV2Output, last bool) bool {
		for _, o := range page.Contents {
			key := decode(*o.Key)
			if key >= fromKey {
				s.Delete(key)
			}
		}
		return true
	})

	return err
}

// We split the key into halves and use the lower half of bits as s3 prefix and
// upper half for the object key. This is to prevent s3 rate limiting which is
// applied to objects with the same prefix.
func encode(key int64) string {
	left := (key >> 32) & 0xffffffff
	right := key & 0xffffffff

	return fmt.Sprintf(keyFmt, right, left)
}

// The inverse to encode()
func decode(keyWithPrefix string) int64 {
	var prefix, key int64
	fmt.Sscanf(keyWithPrefix, keyFmt, &prefix, &key)

	k := (key << 32) + prefix

	return k
}
