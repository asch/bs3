// Copyright (C) 2021 Vojtech Aschenbrenner <v@asch.cz>

package bs3

import (
	"encoding/binary"
	"sync"
	"time"

	"github.com/rs/zerolog/log"

	"github.com/asch/bs3/internal/bs3/key"
	"github.com/asch/bs3/internal/bs3/mapproxy"
	"github.com/asch/bs3/internal/bs3/mapproxy/sectormap"
	"github.com/asch/bs3/internal/bs3/objproxy"
	"github.com/asch/bs3/internal/bs3/objproxy/s3"
	"github.com/asch/bs3/internal/config"
)

const (
	// Size of the metadata for one write in the write chunk read from the
	// kernel.
	WRITE_ITEM_SIZE = 32

	// Key representing the object where serialized version of map is
	// stored.
	checkpointKey = -1

	// Typical number of extents per object for precise memory allocation
	// for return values. In the worst case reallocation happens.
	typicalExtentsPerObject = 128

	// Sector is a linux constant, which is always 512, no matter how big your sectors or blocks
	// are. Please be careful since the terminology is ambiguous.
	sectorUnit = 512
)

// bs3 implements BuseReadWriter interface which can be passed to the buse
// package. Buse package wraps the communication with the BUSE kernel module
// and does all the necessary configuration and low level operations.
//
// bs3 uses s3 protocol to communicate with the storage backend (most probably
// aws s3) but it can be anything else. It manages the mapping between local
// device and remote backend and performs all the operations for correct
// functionality. The default structure is sectormap but it can be changed
// trivially.
type bs3 struct {
	// Proxy struct for the operations on objects like uploads, downloads
	// etc. Proxy structs are used for serialization and prioritization of
	// requests.
	objectStoreProxy objproxy.ObjectProxy

	// Proxy struct for the operations on extent map like updates, lookups
	// etc. Proxy structs are used for serialization and prioritization of
	// requests.
	extentMapProxy mapproxy.ExtentMapProxy

	// Data private to the garbage collection process.
	gcData struct {
		// Reference counter of objects which are actually downloaded
		// and hence cannot be deleted from the storage backend.
		refcounter map[int64]int64

		// Lock guarding the refcounter.
		reflock sync.Mutex
	}

	// Size of the metadata for one write in the write chunk read from the
	// kernel.
	write_item_size int

	// Size of the object portion which contains all writes metadata in the
	// chunk from the kernel. After this metadata_size offset real data are
	// stored.
	metadata_size int
}

// Returns bs3 with default configuration, i.e. with s3 as a communication
// protocol and sectormap as an extent map.
func NewWithDefaults() (*bs3, error) {
	s3Handler, err := s3.New(s3.Options{
		Remote:  config.Cfg.S3.Remote,
		Region:  config.Cfg.S3.Region,
		Profile: config.Cfg.S3.Profile,
		Bucket:  config.Cfg.S3.Bucket,
	})

	if err != nil {
		return nil, err
	}

	mapSize := config.Cfg.Size / int64(config.Cfg.BlockSize)
	bs3 := New(s3Handler, sectormap.New(mapSize))

	return bs3, nil
}

// Returns bs3 with provided protocol for communication with backend storage
// and extentMap for keeping the mapping between local device and remote
// backend.
func New(objectStore objproxy.ObjectUploadDownloaderAt, extentMap mapproxy.ExtentMapper) *bs3 {
	bs3 := bs3{
		objectStoreProxy: objproxy.New(
			objectStore, config.Cfg.S3.Uploaders, config.Cfg.S3.Downloaders,
			time.Duration(config.Cfg.GC.IdleTimeoutMs)*time.Millisecond),

		extentMapProxy: mapproxy.New(
			extentMap, time.Duration(config.Cfg.GC.IdleTimeoutMs)*time.Millisecond),

		metadata_size: config.Cfg.Write.ChunkSize / config.Cfg.BlockSize * WRITE_ITEM_SIZE,

		write_item_size: WRITE_ITEM_SIZE,
	}

	bs3.gcData.refcounter = make(map[int64]int64)

	return &bs3
}

// Handle writes comming from the buse library. writes contain number write
// commands in this call and chunk contains memory where these commands are
// stored together with their data. First part of the chunk are metadata, until
// metadata_size and the rest are data of all writes in the same order.
//
// We read all the writes metadata, create a list and pass it to the extent map
// to update the mapping. Before we actually do that, we wait until the whole
// chunk us uploaded with generated key, which is just one more than the
// previous one.
func (b *bs3) BuseWrite(writes int64, chunk []byte) error {
	key := key.Next()

	metadata := chunk[:b.metadata_size]
	extents := make([]mapproxy.Extent, writes)

	var writtenTotalBlocks uint64
	for i := int64(0); i < writes; i++ {
		e := parseExtent(metadata[:b.write_item_size])
		extents[i] = e
		metadata = metadata[b.write_item_size:]
		writtenTotalBlocks += uint64(e.Length)
	}

	// Zero out the rest of the space reserved for writes. This is because
	// of recovery process, where we lose information about size of the
	// metadata.
	for i := 0; i < len(metadata); i++ {
		metadata[i] = 0
	}

	dataSize := writtenTotalBlocks * uint64(config.Cfg.BlockSize)
	object := chunk[:uint64(b.metadata_size)+dataSize]

	err := b.objectStoreProxy.Upload(key, object, true)
	if err != nil {
		log.Info().Err(err).Send()
	}

	b.extentMapProxy.Update(extents, int64(b.metadata_size/config.Cfg.BlockSize), key)

	return err
}

// Download part of the object to the memory buffer chunk. The part is
// specified by part and it is necessary to call wg.Done() when the upload is
// finished.
func (b *bs3) downloadObjectPart(part mapproxy.ObjectPart, chunk []byte, wg *sync.WaitGroup) {
	defer wg.Done()

	err := b.objectStoreProxy.Download(part.Key, chunk, part.Sector*int64(config.Cfg.BlockSize), true)
	if err != nil {
		log.Info().Err(err).Send()
	}
}

// Read extent starting at sector with length length to the buffer chunk.
// Length of the chunk is the same as length variable. This function consults
// the extent map and asynchronously downloads all needed pieces to reconstruct
// the logical extent.
func (b *bs3) BuseRead(sector, length int64, chunk []byte) error {
	objectPieces := b.getObjectPiecesRefCounterInc(sector, length)

	var wg sync.WaitGroup
	for _, op := range objectPieces {
		size := op.Length * int64(config.Cfg.BlockSize)
		if op.Key != mapproxy.NotMappedKey {
			wg.Add(1)
			go b.downloadObjectPart(op, chunk[:size], &wg)
		}
		chunk = chunk[size:]
	}

	wg.Wait()

	b.objectPiecesRefCounterDec(objectPieces)

	return nil
}

// Before buse library communicating with the kernel starts, we restore map
// stored on the backend and register signal handler of SIGUSR1 which servers
// for threshold garbage collection. Then we run infinite loop with garbage
// collection deleting just completely dead objects withou any data. It is very
// fast and efficiet and has a huge impact on the backend space utilization.
// Hence we run it continuously.
func (b *bs3) BusePreRun() {
	if !config.Cfg.SkipCheckpoint {
		b.restore()
	}

	b.registerSigUSR1Handler()

	go b.gcDead()
}

// After disconnecting from the kernel module and just before shuting the
// daemon down we save the map to the backend so it can be restored during next
// start and mapping is not lost.
func (b *bs3) BusePostRemove() {
	if !config.Cfg.SkipCheckpoint {
		b.checkpoint()
	}
}

// Returns object pieces for reconstructing logical extent but before that
// safely increments the refcounter for the objects. Objects in refcounter are
// excluded from garbage collection.
func (b *bs3) getObjectPiecesRefCounterInc(sector, length int64) []mapproxy.ObjectPart {
	b.gcData.reflock.Lock()
	defer b.gcData.reflock.Unlock()

	objectPieces := b.extentMapProxy.Lookup(int64(sector), int64(length))

	for _, op := range objectPieces {
		b.gcData.refcounter[op.Key]++
	}

	return objectPieces
}

// Decrements the refcounter for the object pieces. Objects in refcounter are
// excluded from garbage collection.
func (b *bs3) objectPiecesRefCounterDec(objectPieces []mapproxy.ObjectPart) {
	b.gcData.reflock.Lock()

	for _, op := range objectPieces {
		b.gcData.refcounter[op.Key]--
	}

	b.gcData.reflock.Unlock()
}

// Restores the map from the checkpoint saved on the backend and updates the
// current object key accordingly. If it exists.
func (b *bs3) restoreFromCheckpoint() {
	mapSize, err := b.objectStoreProxy.Instance.GetObjectSize(checkpointKey)
	if err == nil {
		compressedMap := make([]byte, mapSize)
		b.objectStoreProxy.Download(checkpointKey, compressedMap, 0, false)
		newKey := b.extentMapProxy.Instance.DeserializeAndReturnNextKey(compressedMap)
		key.Replace(newKey)
		log.Info().Int64("key after checkpoint", key.Current()).Send()
	}
}

// Restores the map from individual objects. It reconstructs the map replaying
// all the writes from metadata part of continuous sequence of objects until a
// missing object is found. This is the point where prefix consistency is
// corrupted and we cannot recover more. Any successive objects are deleted.
func (b *bs3) restoreFromObjects() {
	for ; ; key.Next() {
		header := make([]byte, b.metadata_size)
		size, err := b.objectStoreProxy.Instance.GetObjectSize(key.Current())
		if err != nil {
			// Prefix consistency broken.
			break
		}
		if size == 0 {
			// Garbage collected object, that is OK, prefix
			// consistency kept.
			continue
		}

		// Get writes metadata for object.
		err = b.objectStoreProxy.Instance.DownloadAt(key.Current(), header, 0)
		if err != nil {
			break
		}

		// Replay all writes from metadata part until extent with
		// length 0 is found. It is invalid value and it means that the
		// memory is zeroed, which means end of the metadata section of
		// the object. The memory is zeroed out in BuseWrite function
		// where the object is uploaded.
		extents := make([]mapproxy.Extent, 0, typicalExtentsPerObject)
		for {
			e := parseExtent(header[:b.write_item_size])
			if e.Length == 0 {
				break
			}
			extents = append(extents, e)
			header = header[b.write_item_size:]
		}

		dataBegin := int64(b.metadata_size / config.Cfg.BlockSize)
		b.extentMapProxy.Update(extents, dataBegin, key.Current())
	}
	log.Info().Int64("key after roll forward", key.Current()).Send()
}

// Restores map from saved checkpoint and then continuous in restoration from
// individual objects. E.g. when crash happens, checkpoint is not uploaded
// hence the old checkpoint is read. However there can already be uploaded new
// set of objects fulfilling prefix consistency.
func (b *bs3) restore() {
	b.restoreFromCheckpoint()
	b.restoreFromObjects()
	b.objectStoreProxy.Instance.DeleteKeyAndSuccessors(key.Current())
}

// Serializes extent map and upload it to the backend.
func (b *bs3) checkpoint() {
	dump := b.extentMapProxy.Instance.Serialize()
	b.objectStoreProxy.Upload(checkpointKey, dump, false)
}

// Parses write extent information from 32 bytes of raw memory. The memory is
// one write in metadata section of the object.
func parseExtent(b []byte) mapproxy.Extent {
	return mapproxy.Extent{
		Sector: int64(binary.LittleEndian.Uint64(b[:8]) * sectorUnit / uint64(config.Cfg.BlockSize)),
		Length: int64(binary.LittleEndian.Uint64(b[8:16]) * sectorUnit / uint64(config.Cfg.BlockSize)),
		SeqNo:  int64(binary.LittleEndian.Uint64(b[16:24])),
		Flag:   int64(binary.LittleEndian.Uint64(b[24:32])),
	}
}
