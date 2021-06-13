// Copyright (C) 2021 Vojtech Aschenbrenner <v@asch.cz>

// Mapproxy package is a proxy for structs with ExtentMapper interface. It
// serializes and prioritizes requests coming to the ExtentMapper and also
// improves cache locality since all operations are done by the same go
// routine.
package mapproxy

import (
	"time"
)

const (
	NotMappedKey = -1
)

// Provides mapping from logical extents presented in the system to the
// potentionaly mutliple extents in the backend storage. Furthermore it has to
// be provide multiple operations related to garbage collection and map
// restoration.
type ExtentMapper interface {
	Update(extents []Extent, startOfDataSectors, key int64)
	Lookup(sector, length int64) []ObjectPart
	FindExtentsWithKeys(sector, length int64, keys map[int64]struct{}) []ExtentWithObjectPart
	DeleteFromDeadObjects(deadObjects map[int64]struct{})
	DeleteFromUtilization(keys map[int64]struct{})
	GetMaxKey() int64
	ObjectsUtilization() map[int64]int64
	DeadObjects() map[int64]struct{}
	DeserializeAndReturnNextKey(buf []byte) int64
	Serialize() []byte
}

// Proxy to the ExtentMapper. It serializes and prioritizes requests comming to
// the extent map and also improves cache locality since the map is always
// traversed by the same thread.
type ExtentMapProxy struct {
	Instance ExtentMapper

	// Timeout after which low priority request can be handled.
	idleTimeout time.Duration

	// Channels for internal communication specific to one type of request.
	updateChan       chan updateRequest
	lookupChan       chan lookupRequest
	keyedExtentsChan chan keyedExtentsRequest

	// General low priority channel used for multiple types of requests.
	lockChan chan lockRequest
}

// Mapping from the logical extent to the extent in the object.
type ExtentWithObjectPart struct {
	Extent     Extent
	ObjectPart ObjectPart
}

// Logical extent representation representing the system view.
type Extent struct {
	// Beginnig of the extent.
	Sector int64

	// Length of the extent. Extent is continuous.
	Length int64

	// Sequential number of write which wrote this extent
	SeqNo int64

	// Reserved for future usage.
	Flag int64
}

// Object part is extent in the object.
type ObjectPart struct {
	// First sector of the extent.
	Sector int64

	// Length of the extent. Extent is continuous.
	Length int64

	// Object where the extent is located.
	Key int64
}

// Returns proxy which can be directly used. It spawns one worker which handles
// all serialized and prioritized requests.
func New(instance ExtentMapper, idleTimeout time.Duration) ExtentMapProxy {
	updateChan := make(chan updateRequest)
	lookupChan := make(chan lookupRequest)
	keyedExtentsChan := make(chan keyedExtentsRequest)
	lockChan := make(chan lockRequest)

	m := ExtentMapProxy{
		Instance:         instance,
		idleTimeout:      idleTimeout,
		updateChan:       updateChan,
		lookupChan:       lookupChan,
		keyedExtentsChan: keyedExtentsChan,
		lockChan:         lockChan,
	}

	go m.worker()

	return m
}

// Updates all extents specified in extents. startOfDataSectors is the first
// sector in the object with real data and key is the key of the object.
func (p *ExtentMapProxy) Update(extents []Extent, startOfDataSectors, key int64) {
	done := make(chan struct{})
	p.updateChan <- updateRequest{extents, startOfDataSectors, key, done}
	<-done
}

// Finds all pieces from which the logical extent starting from sector with
// length length can be reconstructed.
func (p *ExtentMapProxy) Lookup(sector, length int64) []ObjectPart {
	reply := make(chan []ObjectPart)
	p.lookupChan <- lookupRequest{sector, length, reply}
	return <-reply
}

// Finds all extents which are stored in any of the objects with keys in keys.
// Sector and length is the range of interest.
func (p *ExtentMapProxy) ExtentsInObjects(sector, length int64, keys map[int64]struct{}) []ExtentWithObjectPart {
	reply := make(chan []ExtentWithObjectPart)
	p.keyedExtentsChan <- keyedExtentsRequest{sector, length, keys, reply}
	return <-reply
}

// Returns all dead objects. I.e. objects without any live data.
func (p *ExtentMapProxy) DeadObjects() map[int64]struct{} {
	done := make(chan struct{})
	p.lockChan <- lockRequest{done}
	tmp := p.Instance.DeadObjects()
	<-done

	return tmp
}

// Returns all objects utilization. I.e. number of non-dead sectors in each
// non-dead object.
func (p *ExtentMapProxy) ObjectsUtilization() map[int64]int64 {
	done := make(chan struct{})
	p.lockChan <- lockRequest{done}
	tmp := p.Instance.ObjectsUtilization()
	<-done

	return tmp
}

// Returns highest object key contained in the map.
func (p *ExtentMapProxy) GetMaxKey() int64 {
	done := make(chan struct{})
	p.lockChan <- lockRequest{done}
	tmp := p.Instance.GetMaxKey()
	<-done

	return tmp

}

// Deletes all provided keys from object utilization list.
func (p *ExtentMapProxy) DeleteFromUtilization(keys map[int64]struct{}) {
	done := make(chan struct{})
	p.lockChan <- lockRequest{done}
	defer func() {
		<-done
	}()

	p.Instance.DeleteFromUtilization(keys)
}

// Deletes all dead objects from dead objects list.
func (p *ExtentMapProxy) DeleteDeadObjects(deadObjects map[int64]struct{}) {
	done := make(chan struct{})
	p.lockChan <- lockRequest{done}
	defer func() {
		<-done
	}()

	p.Instance.DeleteFromDeadObjects(deadObjects)
}

type updateRequest struct {
	extents            []Extent
	startOfDataSectors int64
	key                int64
	done               chan struct{}
}

// Internal request structures just for wrapping the function calls into the
// channel communication.

type lookupRequest struct {
	sector int64
	length int64
	reply  chan []ObjectPart
}

type keyedExtentsRequest struct {
	sector int64
	length int64
	keys   map[int64]struct{}
	reply  chan<- []ExtentWithObjectPart
}

type lockRequest struct {
	done chan struct{}
}

// Worker is doing prioritization and serialization of the requests. Updates
// and lookups into the map have highest priority. All other request are low
// priority.
func (p *ExtentMapProxy) worker() {
	for {
		select {
		case u := <-p.updateChan:
			p.update(u)

		case l := <-p.lookupChan:
			p.lookup(l)

		//case <-time.NewTicker(m.idleTimeout).C:
		default:
			select {
			case u := <-p.updateChan:
				p.update(u)

			case l := <-p.lookupChan:
				p.lookup(l)

			case e := <-p.keyedExtentsChan:
				p.findExtensWithKeys(e)

			case l := <-p.lockChan:
				l.done <- struct{}{}
			}
		}
	}
}

func (p *ExtentMapProxy) update(r updateRequest) {
	p.Instance.Update(r.extents, r.startOfDataSectors, r.key)
	r.done <- struct{}{}
}

func (p *ExtentMapProxy) lookup(r lookupRequest) {
	r.reply <- p.Instance.Lookup(r.sector, r.length)
}

func (p *ExtentMapProxy) findExtensWithKeys(r keyedExtentsRequest) {
	r.reply <- p.Instance.FindExtentsWithKeys(r.sector, r.length, r.keys)
}
