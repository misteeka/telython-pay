package eplidr

import (
	"context"
	"database/sql"
	"main/log"
)

type SingleKeyTable struct {
	KeyTable *Table
	key      string
}

func NewSingleKeyTable(name string, key string, driver *sql.DB, params ...string) *SingleKeyTable {
	// params:
	// [0] dataSource
	// [1]
	return &SingleKeyTable{
		KeyTable: NewTable(name, driver, params...),
		key:      key,
	}
}

func SingleKeyImplementation(keyTable *Table, key string) *SingleKeyTable {
	// params:
	// [0] dataSource
	// [1]
	return &SingleKeyTable{
		KeyTable: keyTable,
		key:      key,
	}
}

func columnSliceToString(columns ...string) string {
	result := ""
	for i := 0; i < len(columns); i++ {
		result += " `" + columns[i] + "`"
	}
	return result
}

func (table *SingleKeyTable) GetString(key interface{}, column string) (string, bool, error) {
	return table.KeyTable.GetString(table.key, key, column)
}
func (table *SingleKeyTable) GetInt(key interface{}, column string) (int, bool, error) {
	return table.KeyTable.GetInt(table.key, key, column)
}
func (table *SingleKeyTable) GetInt64(key interface{}, column string) (int64, bool, error) {
	return table.KeyTable.GetInt64(table.key, key, column)
}
func (table *SingleKeyTable) GetFloat(key interface{}, column string) (float64, bool, error) {
	return table.KeyTable.GetFloat(table.key, key, column)
}
func (table *SingleKeyTable) GetUint(key interface{}, column string) (uint64, bool, error) {
	var result uint64
	err, found := table.Get(key, []string{column}, []interface{}{&result})
	if err != nil {
		return 0, found, err
	}
	return result, found, nil
}
func (table *SingleKeyTable) GetBoolean(key interface{}, column string) (bool, bool, error) {
	var result bool
	err, found := table.Get(key, []string{column}, []interface{}{&result})
	if err != nil {
		return false, found, err
	}
	return result, found, nil
}

func (table *SingleKeyTable) Get(key interface{}, columns []string, data []interface{}) (error, bool) {
	return table.KeyTable.Get(table.key, key, columns, data)
}
func (table *SingleKeyTable) Set(key interface{}, columns []string, data []interface{}) error {
	return table.KeyTable.Set(table.key, key, columns, data)
}

func (table *SingleKeyTable) SingleSet(key interface{}, column string, data interface{}) error {
	return table.KeyTable.Set(table.key, key, []string{column}, []interface{}{data})
}

func (table *SingleKeyTable) Put(key interface{}, columns []string, values []interface{}) error {
	return table.KeyTable.Put(append(columns, table.key), append(values, key))
}

func (table *SingleKeyTable) Remove(key interface{}) error {
	return table.KeyTable.Remove(table.key, key)
}

func (table *SingleKeyTable) ReleaseRows(rows *sql.Rows) {
	err := rows.Close()
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return
	}
}

func (table *SingleKeyTable) Begin() (*Tx, error) {
	driver, err := table.KeyTable.Driver.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{
		table:  table.KeyTable,
		driver: driver,
	}, nil
}

func (table *SingleKeyTable) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return table.KeyTable.Driver.BeginTx(ctx, opts)
}

func (table *SingleKeyTable) ExecTx(query ...string) error {
	tx, err := table.KeyTable.Driver.Begin()
	if err != nil {
		return err
	}
	for i := 0; i < len(query); i++ {
		_, err = tx.Exec(query[i])
		if err != nil {
			return err
		}
	}
	return nil
}
