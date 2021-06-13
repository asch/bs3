// Copyright (C) 2021 Vojtech Aschenbrenner <v@asch.cz>

// Null package does nothing but correctly.
package null

// Null implementation of BuseReadWriter. Usefull for measuring performance of
// underlying BUSE and buse library. Otherwise useless. Is contained in the
// same module to avoid duplication in BUSE code and configuration. It can also
// serve as a template for new BUSE device implementation since it is an
// implementation of BuseReadWriter interface.
type null struct {
}

func NewNull() *null {
	return &null{}
}

func (n *null) BuseWrite(writes int64, chunk []byte) error {
	return nil
}

func (n *null) BuseRead(sector, length int64, chunk []byte) error {
	return nil
}

func (n *null) BusePreRun() {
}

func (n *null) BusePostRemove() {
}
