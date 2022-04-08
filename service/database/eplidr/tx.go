package eplidr

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type Tx struct {
	table   *Table
	drivers map[uint]*sql.Tx
}

// keys := make([]keyType, 0, len(myMap))
// values := make([]valueType, 0, len(myMap))

func (tx *Tx) GetString(keyName interface{}, key interface{}, column string) (string, bool, error) {
	var result string
	err, found := tx.Get(keyName, key, []string{column}, []interface{}{&result})
	if err != nil {
		return "", found, err
	}
	return result, found, nil
}
func (tx *Tx) GetInt(keyName interface{}, key interface{}, column string) (int, bool, error) {
	var result int
	err, found := tx.Get(keyName, key, []string{column}, []interface{}{&result})
	if err != nil {
		return 0, found, err
	}
	return result, found, nil
}
func (tx *Tx) GetInt64(keyName interface{}, key interface{}, column string) (int64, bool, error) {
	var result int64
	err, found := tx.Get(keyName, key, []string{column}, []interface{}{&result})
	if err != nil {
		return 0, found, err
	}
	return result, found, nil
}
func (tx *Tx) GetFloat(keyName interface{}, key interface{}, column string) (float64, bool, error) {
	var result float64
	err, found := tx.Get(keyName, key, []string{column}, []interface{}{&result})
	if err != nil {
		return 0, found, err
	}
	return result, found, nil
}
func (tx *Tx) GetUint(keyName interface{}, key interface{}, column string) (uint64, bool, error) {
	var result uint64
	err, found := tx.Get(keyName, key, []string{column}, []interface{}{&result})
	if err != nil {
		return 0, found, err
	}
	return result, found, nil
}
func (tx *Tx) GetBoolean(keyName interface{}, key interface{}, column string) (bool, bool, error) {
	var result bool
	err, found := tx.Get(keyName, key, []string{column}, []interface{}{&result})
	if err != nil {
		return false, found, err
	}
	return result, found, nil
}

func (tx *Tx) getDriver(shard uint) (*sql.Tx, error) {
	driver, ok := tx.drivers[shard]
	var err error
	if !ok {
		driver, err = tx.table.Drivers[shard].Begin()
		if err != nil {
			return nil, err
		}
		tx.drivers[shard] = driver
	}
	return driver, nil
}

func (tx *Tx) Get(keyName interface{}, key interface{}, columns []string, data []interface{}) (error, bool) {
	shard := tx.table.getShard(key)
	query := fmt.Sprintf("SELECT %s FROM `%s` WHERE `%v` = %s FOR UPDATE;", columnSliceToString(columns...), tx.table.getName(shard), keyName, value(key))
	rows, err := tx.Query(query, key)
	if err != nil {
		rows.Close()
		return err, false
	}
	if rows.Next() {
		err := rows.Scan(data...)
		if err != nil {
			rows.Close()
			return err, true
		}
		rows.Close()
	} else {
		rows.Close()
		return nil, false
	}
	return nil, true
}
func (tx *Tx) Put(keyName interface{}, key interface{}, columns []string, values []interface{}) error {
	shard := tx.table.getShard(key)
	driver, err := tx.getDriver(shard)
	if err != nil {
		return err
	}
	if len(columns) != len(values) {
		return errors.New("keyTable.Put : len(columns) != len(data) ")
	}
	columnsString := ""
	valuesString := ""
	for i := 0; i < len(columns); i++ {
		if i == len(columns)-1 {
			columnsString += fmt.Sprintf("`%s`", columns[i])
		} else {
			columnsString += fmt.Sprintf("`%s`, ", columns[i])
		}
	}
	for i := 0; i < len(values); i++ {
		if i == len(values)-1 {
			valuesString += fmt.Sprintf("%s", value(values[i]))
		} else {
			valuesString += fmt.Sprintf("%s, ", value(values[i]))
		}
	}
	query := fmt.Sprintf("INSERT INTO `%s` (%s) values (%s);", tx.table.name, columnsString, valuesString)
	_, err = driver.Exec(query)
	if err != nil {
		return err
	}
	return nil
}
func (tx *Tx) Set(keyName interface{}, key interface{}, columns []string, values []interface{}) error {
	shard := tx.table.getShard(key)
	driver, err := tx.getDriver(shard)
	if err != nil {
		return err
	}
	if len(columns) != len(values) {
		return errors.New("keyTable.Set : len(columns) != len(values) ")
	}
	s := ""
	for i := 0; i < len(columns); i++ {
		if i == len(columns)-1 {
			s += fmt.Sprintf("`%s` = %s", columns[i], value(values[i]))
		} else {
			s += fmt.Sprintf("`%s` = %s, ", columns[i], value(values[i]))
		}
	}
	query := fmt.Sprintf("UPDATE `%s` SET %s WHERE `%s` = %s;", tx.table.name, s, keyName, value(key))
	_, err = driver.Exec(query)
	if err != nil {
		return err
	}
	return nil
}
func (tx *Tx) Remove(keyName interface{}, key interface{}) error {
	shard := tx.table.getShard(key)
	query := fmt.Sprintf("DELETE FROM `%s` WHERE `%s` = %s;", tx.table.getName(shard), keyName, value(key))
	_, err := tx.drivers[shard].Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func (tx *Tx) Exec(query string, key interface{}) (sql.Result, error) {
	shard := tx.table.getShard(key)
	driver, err := tx.getDriver(shard)
	if err != nil {
		return nil, err
	}
	query = strings.ReplaceAll(query, "{table}", fmt.Sprintf("`%s`", tx.table.getName(shard)))
	return driver.Exec(query)
}

func (tx *Tx) Query(query string, key interface{}) (*sql.Rows, error) {
	shard := tx.table.getShard(key)
	driver, err := tx.getDriver(shard)
	if err != nil {
		return nil, err
	}
	query = strings.ReplaceAll(query, "{table}", fmt.Sprintf("`%s`", tx.table.getName(shard)))
	return driver.Query(query)
}

func (tx *Tx) SingleSet(keyName string, key interface{}, column string, value interface{}) error {
	return tx.Set(keyName, key, []string{column}, []interface{}{value})
}

func (tx *Tx) Commit() error {
	return nil
}

func (tx *Tx) Rollback() error {
	return nil
	//return tx.driver.Rollback()
}
func (tx *Tx) Fail() {
	//tx.driver.Rollback()
}
