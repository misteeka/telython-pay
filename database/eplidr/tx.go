package eplidr

import (
	"database/sql"
	"errors"
	"fmt"
	"main/log"
	"strings"
)

type Tx struct {
	table  *Table
	driver *sql.Tx
}

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

func (tx *Tx) Get(keyName interface{}, key interface{}, columns []string, data []interface{}) (error, bool) {
	query := fmt.Sprintf("SELECT %s FROM `%s` WHERE `%v` = %s FOR UPDATE;", columnSliceToString(columns...), tx.table.name, keyName, value(key))
	rows, err := tx.driver.Query(query)
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
func (tx *Tx) Put(columns []string, values []interface{}) error {
	// `%s` = ?
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
	_, err := tx.driver.Exec(query)
	if err != nil {
		return err
	}
	return nil
}
func (tx *Tx) Set(keyName interface{}, key interface{}, columns []string, values []interface{}) error {
	// `%s` = ?
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
	_, err := tx.driver.Exec(query)
	if err != nil {
		return err
	}
	return nil
}
func (tx *Tx) Remove(keyName interface{}, key interface{}) error {
	query := fmt.Sprintf("DELETE FROM `%s` WHERE `%s` = %s;", tx.table.name, keyName, value(key))
	_, err := tx.driver.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func (tx *Tx) Exec(query string, args ...any) (sql.Result, error) {
	query = strings.ReplaceAll(query, "{table_name}", fmt.Sprintf("`%s`", tx.table.name))
	return tx.driver.Exec(query, args...)
}

func (tx *Tx) ExecSlice(query ...string) (sql.Result, error) {
	finalQuery := strings.Join(query, "\n")
	finalQuery = strings.ReplaceAll(finalQuery, "{table_name}", fmt.Sprintf("`%s`", tx.table.name))
	log.InfoLogger.Println(finalQuery)
	return tx.driver.Exec(finalQuery)
}

func (tx *Tx) Query(query string, args ...any) (*sql.Rows, error) {
	query = strings.ReplaceAll(query, "{table_name}", fmt.Sprintf("`%s`", tx.table.name))
	return tx.driver.Query(query, args...)
}

func (tx *Tx) SingleSet(keyName string, key interface{}, column string, value interface{}) error {
	return tx.Set(keyName, key, []string{column}, []interface{}{value})
}

func (tx *Tx) Commit() error {
	return tx.driver.Commit()
}

func (tx *Tx) Rollback() error {
	return tx.driver.Rollback()
}
func (tx *Tx) Fail() {
	tx.driver.Rollback()
}
