// Copyright (C) 2021 Vojtech Aschenbrenner <v@asch.cz>

package bs3

import (
	"encoding/binary"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/asch/bs3/internal/bs3/key"
	"github.com/asch/bs3/internal/bs3/mapproxy"
	"github.com/asch/bs3/internal/config"

	"github.com/rs/zerolog/log"
)

const (
	// Typical number of newly created objects during one threshold GC run.
	// Just an optimization of memory allocation, in the worst case
	// reallocation occurs.
	typicalNewObjectsPerGC = 64

	// Typical number of extents per one garbage collected object. Just an
	// optimization of memory allocation, in the worst case reallocation
	// occurs.
	typicalExtentsPerGCObject = 64
)

// Select objects viable for threshold GC. When an object utilization is under
// the threshold it is selected for GC. The object with the highest key is
// never collected because of oscilation.
func (b *bs3) filterKeysToCollect(utilization map[int64]int64, ratio float64) map[int64]struct{} {
	var maxKey int64
	collect := make(map[int64]struct{})

	for k, v := range utilization {
		used := v * int64(config.Cfg.BlockSize)
		r := float64(used) / float64(config.Cfg.Write.ChunkSize)
		if r < ratio {
			collect[k] = struct{}{}
		}

		if k > maxKey {
			maxKey = k
		}
	}

	if _, ok := collect[maxKey]; ok {
		delete(collect, maxKey)
	}

	return collect
}

// Constructs the list of life extents to be saved from objects subjected to the GC.
func (b *bs3) getCompleteWriteList(keys map[int64]struct{}, stepSize int64) []mapproxy.ExtentWithObjectPart {
	completeWriteList := make([]mapproxy.ExtentWithObjectPart, 0, 128)

	sectors := config.Cfg.Size / int64(config.Cfg.BlockSize)

	for i := int64(0); i < sectors; i += stepSize {
		ci := b.extentMapProxy.ExtentsInObjects(int64(i), stepSize, keys)

		if len(ci) == 0 {
			continue
		}

		completeWriteList = append(completeWriteList, ci...)

	}

	return completeWriteList
}

// Removes currently downloaded objects from the list of dead objects.
func (b *bs3) filterDownloadingObjects(deadObjects map[int64]struct{}) {
	b.gcData.reflock.Lock()
	defer b.gcData.reflock.Unlock()

	for k, v := range b.gcData.refcounter {
		if v == 0 {
			delete(b.gcData.refcounter, k)
		} else {
			_, ok := deadObjects[k]
			if ok {
				delete(deadObjects, k)
			}
		}
	}
}

// Runs threshold GC. It makes all objects with live data ratio under the
// threshold dead by copying their live data into new object. These objects are
// deleted during the regular dead GC run.
func (b *bs3) gcThreshold(stepSize int64, threshHold float64) {
	liveObjects := b.extentMapProxy.ObjectsUtilization()
	keysToCollect := b.filterKeysToCollect(liveObjects, threshHold)
	completeWritelist := b.getCompleteWriteList(keysToCollect, stepSize)
	objects, extents := b.composeObjects(completeWritelist)

	for i := range objects {
		key := key.Next()

		err := b.objectStoreProxy.Upload(key, objects[i], false)
		if err != nil {
			log.Info().Err(err).Send()
		}

		b.extentMapProxy.Update(extents[i], int64(b.metadata_size/config.Cfg.BlockSize), key)
	}
}

// Removes unneeded dead objects from the map and upload empty object instead.
// The object cannot be deleted on the backend, because the sequence number
// would be missing in the recovery process where we need continuous range of
// keys.
func (b *bs3) removeNonReferencedDeadObjects() {
	deadObjects := b.extentMapProxy.DeadObjects()
	b.filterDownloadingObjects(deadObjects)
	for k := range deadObjects {
		err := b.objectStoreProxy.Upload(k, []byte{}, false)
		if err != nil {
			log.Info().Err(err).Send()
		}
	}
	b.extentMapProxy.DeleteDeadObjects(deadObjects)
}

// Register SIGUSR1 as a trigger for threshold GC.
func (b *bs3) registerSigUSR1Handler() {
	gcChan := make(chan os.Signal, 1)
	signal.Notify(gcChan, syscall.SIGUSR1)

	go func() {
		for range gcChan {
			log.Info().Msgf("Threshold GC started with threshold %1.2f.", config.Cfg.GC.LiveData)
			b.gcThreshold(config.Cfg.GC.Step, config.Cfg.GC.LiveData)
			log.Info().Msg("Threshold GC finished.")
		}
	}()
}

// Dead GC infinite loop. Highly efficient hence running regularly.
func (b *bs3) gcDead() {
	for {
		time.Sleep(time.Duration(config.Cfg.GC.Wait) * time.Second)

		log.Trace().Msg("Dead GC started.")
		b.removeNonReferencedDeadObjects()
		log.Trace().Msg("Dead GC finished.")
	}
}

// Stores raw values of individual write into metadata part of the object.
func writeHeader(metadataFrontier int, g mapproxy.ExtentWithObjectPart, object []byte) {
	binary.LittleEndian.PutUint64(object[metadataFrontier:], uint64(g.ObjectPart.Sector))
	metadataFrontier += 8

	binary.LittleEndian.PutUint64(object[metadataFrontier:], uint64(g.Extent.Length))
	metadataFrontier += 8

	binary.LittleEndian.PutUint64(object[metadataFrontier:], uint64(g.Extent.SeqNo))
	metadataFrontier += 8

	binary.LittleEndian.PutUint64(object[metadataFrontier:], uint64(g.Extent.Flag))
	metadataFrontier += 8
}

// Traverse the list of all extents which are going to be copied into new fresh
// object(s). It downloads necessary parts and constructs new objects for the
// complete list. All objects are then uploaded and map updated.
func (b *bs3) composeObjects(writeList []mapproxy.ExtentWithObjectPart) ([][]byte, [][]mapproxy.Extent) {
	var wg sync.WaitGroup

	metadataFrontier := 0
	dataFrontier := b.metadata_size

	objects := make([][]byte, 0, typicalNewObjectsPerGC)
	extents := make([][]mapproxy.Extent, 0, typicalNewObjectsPerGC)

	object := make([]byte, config.Cfg.Write.ChunkSize)
	currentObjectExtents := make([]mapproxy.Extent, 0, typicalExtentsPerGCObject)

	for _, g := range writeList {
		if uint64(dataFrontier)+uint64(g.Extent.Length)*uint64(config.Cfg.BlockSize) > uint64(config.Cfg.Write.ChunkSize) {
			objects = append(objects, object)
			extents = append(extents, currentObjectExtents)

			object = make([]byte, config.Cfg.Write.ChunkSize)
			currentObjectExtents = make([]mapproxy.Extent, 0, typicalExtentsPerGCObject)

			metadataFrontier = 0
			dataFrontier = b.metadata_size
		}

		writeHeader(metadataFrontier, g, object)
		metadataFrontier += b.write_item_size

		data := object[dataFrontier : int64(dataFrontier)+g.Extent.Length*int64(config.Cfg.BlockSize)]
		wg.Add(1)
		go func(g mapproxy.ExtentWithObjectPart) {
			defer wg.Done()
			err := b.objectStoreProxy.Download(g.ObjectPart.Key, data, g.Extent.Sector*int64(config.Cfg.BlockSize), true)
			if err != nil {
				log.Info().Err(err).Send()
			}
		}(g)

		extent := mapproxy.Extent{
			Sector: g.ObjectPart.Sector,
			Length: g.Extent.Length,
			SeqNo:  g.Extent.SeqNo,
			Flag:   g.Extent.Flag,
		}

		currentObjectExtents = append(currentObjectExtents, extent)
		dataFrontier += int(g.Extent.Length) * config.Cfg.BlockSize
	}

	if len(currentObjectExtents) > 0 {
		objects = append(objects, object)
		extents = append(extents, currentObjectExtents)
	}

	wg.Wait()

	return objects, extents
}
