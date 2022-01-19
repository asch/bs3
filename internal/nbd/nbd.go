package nbd

import (
	"encoding/binary"

	"github.com/asch/bs3/internal/bs3/mapproxy"
	"github.com/asch/bs3/internal/config"
	"libguestfs.org/libnbd"
)

const (
	WRITE_ITEM_SIZE = 32
)

type nbd struct {
	handle          *libnbd.Libnbd
	metadata_size   int
	write_item_size int
}

func NewNbd() *nbd {
	return &nbd{
		metadata_size:   config.Cfg.Write.ChunkSize / config.Cfg.BlockSize * WRITE_ITEM_SIZE,
		write_item_size: WRITE_ITEM_SIZE,
	}
}

// Parses write extent information from 32 bytes of raw memory. The memory is
// one write in metadata section of the object.
func parseExtent(b []byte) mapproxy.Extent {
	return mapproxy.Extent{
		Sector: int64(binary.LittleEndian.Uint64(b[:8]) * 512 / uint64(config.Cfg.BlockSize)),
		Length: int64(binary.LittleEndian.Uint64(b[8:16]) * 512 / uint64(config.Cfg.BlockSize)),
		SeqNo:  int64(binary.LittleEndian.Uint64(b[16:24])),
		Flag:   int64(binary.LittleEndian.Uint64(b[24:32])),
	}
}

func (n *nbd) BuseWrite(writes int64, chunk []byte) error {
	metadata := chunk[:n.metadata_size]

	data := chunk[n.metadata_size:]
	for i := int64(0); i < writes; i++ {
		e := parseExtent(metadata[:n.write_item_size])

		n.handle.Pwrite(data[:e.Length*512], uint64(e.Sector*512), nil)

		metadata = metadata[n.write_item_size:]
		data = data[e.Length*512:]
	}

	return nil
}

func (n *nbd) BuseRead(sector, length int64, chunk []byte) error {
	err := n.handle.Pread(chunk, uint64(sector), nil)
	return err
}

func (n *nbd) BusePreRun() {
	var err error
	n.handle, err = libnbd.Create()
	if err != nil {
		panic(err)
	}

	err = n.handle.ConnectUnix("/tmp/nbd.sock")
	if err != nil {
		panic(err)
	}
}

func (n *nbd) BusePostRemove() {
	n.handle.Close()
}
