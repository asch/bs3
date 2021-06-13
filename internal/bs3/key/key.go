// Copyright (C) 2021 Vojtech Aschenbrenner <v@asch.cz>

// Package for synchronized access to the object key counter.
package key

import (
	"sync"
)

var (
	key   int64
	mutex sync.Mutex
)

// Returns value of currently unassigned key. It is forbidden to use this key
// for creating a new object withou calling Next() function. I.e. this key can
// be used for the next object.
func Current() int64 {
	mutex.Lock()
	defer mutex.Unlock()

	return key
}

// Returns value of currently unassigned key and increments, hence the key
// variable contains unassigned key again.. I.e. this key can be used for the
// next object.
func Next() int64 {
	mutex.Lock()
	defer mutex.Unlock()

	tmp := key
	key++

	return tmp
}

// Replaces the value of the next unassigned key.
func Replace(newKey int64) {
	mutex.Lock()
	defer mutex.Unlock()

	key = newKey
}
