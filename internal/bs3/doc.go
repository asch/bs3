// Copyright (C) 2021 Vojtech Aschenbrenner <v@asch.cz>

// bs3 is a userspace daemon using golang buse library to create a block
// device. All operations on the block device are handled by the daemon. bs3
// stores data in object storage via s3 protocol and maintains the mapping
// between logical block device space and the backend.
//
// bs3 defines two interfaces. One for the extent map and one for the storage
// backend operations. These two parts can be trivially changed just by
// implementing corresponding interface.
package bs3
