package eplidr

import (
	"database/sql"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"strconv"
	"strings"
)

type DB struct {
	Drivers          []*sql.DB
	tableName        string
	createTableQuery string

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
func (db *DB) createTables() {
	for i := 0; i < len(db.Drivers); i++ {
		_, err := db.Drivers[i].Exec(strings.Replace(db.createTableQuery, "{table_name}", db.tableName+strconv.Itoa(i), 1))
		if err != nil {
			fmt.Println(err.Error())
			return
		}
	}
}

func New(tableName string, shardsCount uint, createTableQuery string, drivers []*sql.DB) *DB {
	db := DB{
		Drivers:          drivers,
		createTableQuery: createTableQuery,
		tableName:        tableName,
		shardsCount:      shardsCount,
		hashFunc:         StandardGetShardFunc,
	}
	db.createTables()
	return &db
}

func (db *DB) Exec(query string, key string, args ...interface{}) (sql.Result, error) {
	shardNum := db.getShardFunc(key)
	query = strings.Replace(query, "{table_name}", db.tableName+strconv.Itoa(int(shardNum)), 1)
	query = strings.Replace(query, "{key}", key, 1)
	return db.Drivers[shardNum].Exec(query, args...)
}

func (db *DB) Query(query string, key string, args ...interface{}) (*sql.Rows, error) {
	shardNum := db.getShardFunc(key)
	query = strings.Replace(query, "{table_name}", db.tableName+strconv.Itoa(int(shardNum)), 1)
	query = strings.Replace(query, "{key}", key, 1)
	return db.Drivers[shardNum].Query(query, args...)
}
