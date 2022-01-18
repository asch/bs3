package nbd

import (
	"fmt"

	"libguestfs.org/libnbd"
)

type nbd struct {
	handle *libnbd.Libnbd
}

func NewNbd() *nbd {
	return &nbd{}
}

func (n *nbd) BuseWrite(writes int64, chunk []byte) error {
	// TODO
	return nil
}

func (n *nbd) BuseRead(sector, length int64, chunk []byte) error {
	fmt.Println("Reading" + string(sector))
	err := n.handle.Pread(chunk, uint64(sector), nil)
	fmt.Println(err)

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
