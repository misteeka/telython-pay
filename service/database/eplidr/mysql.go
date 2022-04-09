package eplidr

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

type DB struct {
	Tables []*Table

	shardsCount uint

	hashFunc func(interface{}) uint
}

var shiftTableIterator int

func init() {
	shiftTableIterator = 1
}

func StandardGetShardFunc(key interface{}) uint {
	return fnv32(fmt.Sprintf("%v", key))
}
func fnv32(key string) uint {
	hash := uint(2166136261)
	const prime32 = uint(16777619)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime32
		hash ^= uint(key[i])
	}
	return hash
}
func (db *DB) getShardFunc(key interface{}) uint {
	return db.hashFunc(key) % db.shardsCount
}

func Begin() {

}
