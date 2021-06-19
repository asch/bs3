// Copyright (C) 2021 Vojtech Aschenbrenner <v@asch.cz>

// Sectormap package provides implementation of ExtentMapper interface. It
// implements high efficient mapping with sector granularity. More details are
// in the SectorMap struct description.
package sectormap

import (
	"bytes"
	"encoding/gob"

	"github.com/asch/bs3/internal/bs3/mapproxy"
)

const (
	// How many objects parts is the typical result for one extent lookup.
	// This is just for initial allocation of the returned array. In the
	// worst case reallocation happens.
	typicalObjectPartsPerLookup = 64

	notMappedKey = -1
)

// Description of the sector. It provides information about corresponding
// sector in the object and object identification.
type SectorMetadata struct {
	// Sector in the object.
	Sector int64

	// Key of the object.
	Key int64

	// Sequential number of the last write to this sector.
	SeqNo int64

	// Reserved for future usage.
	Flag int64
}

// Implementation of the ExtentMapper interface hence serving as and extent map. This is high
// efficient mapping of individual sectors stored in an continuous array. The obvious advantage is
// speed, where linear scanning of array is incredibly fast operation on modern CPUs. The second
// advantage is simplicity. The disadvantage can be that it consumes still the same amount of
// memory, no matter how used the device is. However the worst case memory usage is the best
// possible because we don't store any additional data like in some more complex data structures
// like trees.
//
// Nevertheless if the memory usage is a problem, slightly raising the sector size helps
// tremendously. 4k sectors are norm today and if we have 1TB block device the map consumes
// 1TB/4k*32 = 8GB. With 8k sectors it is just 4GB. This can be further reduced by shrinking data
// types in SectorMetadata structure from int64 which is an overkill for most of them.
//
// This structure is serialized by gobs hence it has to be exported and all its attributes as well.
type SectorMap struct {
	Sectors         []SectorMetadata
	ObjUtilizations map[int64]int64
	DeadObjs        map[int64]struct{}
}

// Returns new instance of the sector map. The map should not be used directly because it does not
// support concurrent access.
func New(length int64) *SectorMap {
	sectors := make([]SectorMetadata, length)
	objectUtilization := make(map[int64]int64)
	deadObjects := make(map[int64]struct{})

	for i := range sectors {
		sectors[i].Key = notMappedKey
	}

	s := SectorMap{
		Sectors:         sectors,
		ObjUtilizations: objectUtilization,
		DeadObjs:        deadObjects,
	}

	return &s
}

// Updates sectors in the map with new values from extents. startOfDataSectors
// is the first sector with data in the object and key is the key of the
// object.
func (m *SectorMap) Update(extents []mapproxy.Extent, startOfDataSectors, key int64) {
	m.ObjUtilizations[key] = 0

	for _, e := range extents {
		m.updateExtent(e, startOfDataSectors, key)
		startOfDataSectors += e.Length
	}

	// Because of GC we can add object which will never update the map
	// because all write records are old
	if m.ObjUtilizations[key] == 0 {
		delete(m.ObjUtilizations, key)
		m.DeadObjs[key] = struct{}{}
	}
}

// Updates the information about objects utilizations for given sector.
func (m *SectorMap) updateUtilization(key int64, s *SectorMetadata) {
	// Increment cannot be done at once because GC can
	// introduce object with writes with lower seqNo
	m.ObjUtilizations[key]++
	if s.Key != notMappedKey {
		m.ObjUtilizations[s.Key]--
		if m.ObjUtilizations[s.Key] == 0 {
			delete(m.ObjUtilizations, s.Key)
			m.DeadObjs[s.Key] = struct{}{}
		}
	}
}

// Update one sector.
func (m *SectorMap) updateSector(key int64, s *SectorMetadata, targetSector int64, e mapproxy.Extent) {
	m.updateUtilization(key, s)

	s.Sector = targetSector
	s.Key = key
	s.SeqNo = e.SeqNo
	s.Flag = e.Flag
}

// Updates an extent. It checks whether the write is actually newer than write
// already in the map. Like this we always keep the map consistent.
func (m *SectorMap) updateExtent(e mapproxy.Extent, startOfDataSectors, key int64) {
	targetSector := startOfDataSectors
	for i := e.Sector; i < e.Sector+e.Length; i++ {
		s := &m.Sectors[i]
		if s.SeqNo <= e.SeqNo { // Equality because of GC
			m.updateSector(key, s, targetSector, e)
		}
		targetSector++
	}
}

// Returns longest possible extent in the object starting at startSector with
// maximal length length. This means that the extent has the same key and
// sequential number.
func (m *SectorMap) getExtent(startSector, length uint64) mapproxy.Extent {
	s := m.Sectors[startSector]
	e := mapproxy.Extent{
		Sector: s.Sector,
		Length: 1,
		SeqNo:  s.SeqNo,
		Flag:   s.Flag,
	}

	for i := startSector + 1; ; i++ {
		if i >= uint64(len(m.Sectors)) ||
			i >= startSector+length ||
			m.Sectors[i].Key != m.Sectors[i-1].Key ||
			m.Sectors[i].SeqNo != e.SeqNo ||
			m.Sectors[i-1].Sector != m.Sectors[i].Sector-1 {

			break
		}

		e.Length++
	}

	return e
}

// Returns all ObjectParts from which extent starting at sector with length
// length can be reconstructed.
func (m *SectorMap) Lookup(sector, length int64) []mapproxy.ObjectPart {
	parts := make([]mapproxy.ObjectPart, 0, typicalObjectPartsPerLookup)
	s := m.Sectors[sector].Sector
	l := int64(1)
	for i := int64(1); i < length; i++ {
		id := sector + i
		// The next sector is not from the same extent. Store part into
		// the returned value and begin new extent.
		if (m.Sectors[id].Key != m.Sectors[id-1].Key ||
			m.Sectors[id].Sector != m.Sectors[id-1].Sector+1) &&
			(m.Sectors[id].Key != -1 || m.Sectors[id-1].Key != notMappedKey) {

			parts = append(parts, mapproxy.ObjectPart{
				Sector: s,
				Length: l,
				Key:    m.Sectors[id-1].Key,
			})
			s = m.Sectors[id].Sector
			l = 1
		} else {
			l++
		}
	}
	parts = append(parts, mapproxy.ObjectPart{
		Sector: s,
		Length: l,
		Key:    m.Sectors[sector+length-1].Key,
	})
	return parts
}

// Returns all extents and objectparts starting from sector with length length
// that are stored in any of keys in keys.
func (m *SectorMap) FindExtentsWithKeys(sector, length int64, keys map[int64]struct{}) []mapproxy.ExtentWithObjectPart {
	ci := make([]mapproxy.ExtentWithObjectPart, 0, typicalObjectPartsPerLookup)

	for i := sector; i < sector+length && i < int64(len(m.Sectors)); {
		key := m.Sectors[i].Key
		_, ok := keys[key]
		extent := m.getExtent(uint64(i), uint64(sector+length-i))
		if ok {
			op := mapproxy.ObjectPart{
				Sector: i,
				Length: 0,
				Key:    key,
			}
			ci = append(ci, mapproxy.ExtentWithObjectPart{
				Extent:     extent,
				ObjectPart: op,
			})
		}
		i += extent.Length
	}

	return ci
}

// Returns copy of deadObjects. These are objects with no valid data which can
// be deleted.
func (m *SectorMap) DeadObjects() map[int64]struct{} {
	deadObjects := make(map[int64]struct{})

	for k := range m.DeadObjs {
		deadObjects[k] = struct{}{}
	}

	return deadObjects
}

// Returns the highest key from the map.
func (m *SectorMap) GetMaxKey() int64 {
	var maxKey int64
	for k := range m.ObjUtilizations {
		if k > maxKey {
			maxKey = k
		}
	}

	return maxKey
}

// Return copy of the structure representing the object utilization.
// Utilization is number of non-dead sectors.
func (m *SectorMap) ObjectsUtilization() map[int64]int64 {
	objectUtilization := make(map[int64]int64)

	for k, v := range m.ObjUtilizations {
		objectUtilization[k] = v
	}

	return objectUtilization
}

// Returns serialized version of the map with go gobs.
func (m *SectorMap) Serialize() []byte {
	var buf bytes.Buffer

	encoder := gob.NewEncoder(&buf)
	encoder.Encode(m)

	return buf.Bytes()
}

// Deserialized map from buf which was previously serialized by Serialize(). It
// restored map and structures representing object utilization and dead
// objects. During deserialization all sequential numbers are zeroed because
// most they are not needed and most probably BUSE starts from 0 since it was
// restarted. The map supports device size change.
func (m *SectorMap) DeserializeAndReturnNextKey(buf []byte) int64 {
	// Size of the allocated map
	intendedSize := len(m.Sectors)

	// 1) In case of smaller checkpointed map, i.e. we enlarged the device,
	//    the map would be shrinked and we need to resize it to its
	//    intended size.
	// 2) In case of larger checkpointed map, i.e. we shrinked the device,
	//    the map would be enlarged and we need to resize it to its inteded size.
	decoder := gob.NewDecoder(bytes.NewReader(buf))
	decoder.Decode(m)

	if intendedSize < len(m.Sectors) {
		// Create new map with smaller size and copy the intended range
		// to it. Then replace the the map. We could just change the
		// len of the map, but then the memory would be still occupied
		// like in the case of larger map.
		sectors := make([]SectorMetadata, intendedSize)
		copy(sectors, m.Sectors)
		m.Sectors = sectors
	} else {
		// We already have allocated large map, but we decoded smaller
		// one and it the len was set according to the decoded
		// (smaller) map. We just change len to its full size.
		m.Sectors = m.Sectors[:cap(m.Sectors)]
	}

	var maxKey int64 = notMappedKey
	for _, s := range m.Sectors {
		if s.Key > maxKey {
			maxKey = s.Key
		}
	}

	for i := range m.Sectors {
		m.Sectors[i].SeqNo = 0
	}

	return maxKey + 1
}

// Deletes objects with keys from object utilizations.
func (m *SectorMap) DeleteFromUtilization(keys map[int64]struct{}) {
	for k := range keys {
		delete(m.ObjUtilizations, k)
	}
}

// Deletes objects with keys from deadObjects from dead objects.
func (m *SectorMap) DeleteFromDeadObjects(deadObjects map[int64]struct{}) {
	for k := range deadObjects {
		_, ok := m.DeadObjs[k]
		if ok {
			delete(m.DeadObjs, k)
		}
	}
}
