package eplidr

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"main/log"
	"strconv"
	"strings"
)

type Table struct {
	name   string
	Driver *sql.DB
}

func NewTable(name string, driver *sql.DB, params ...string) *Table {
	// params:
	// [0] dataSource
	// [1]
	return &Table{
		name:   name,
		Driver: driver,
	}
}

func Serialize(value interface{}) string {
	switch array := value.(type) {
	case map[string]interface{}:
		var result string
		keys := make([]string, 0, len(array))
		values := make([]interface{}, 0, len(array))
		for i := 0; i < len(array); i++ {
			if i == len(array)-1 {
				result += fmt.Sprintf("%s:%v", keys[i], values[i])
			} else {
				result += fmt.Sprintf("%s:%v,", keys[i], values[i])
			}
		}
		return result
	case []interface{}:
		var result string
		for i := 0; i < len(array); i++ {
			if i == len(array)-1 {
				result += fmt.Sprintf("%v", array[i])
			} else {
				result += fmt.Sprintf("%v,", array[i])
			}
		}
		return result
	default:
		return ""
	}
}
func DeserializeMap(serializedData string) {
	var result map[string]interface{}
	for i := 0; i < len(serializedData); i++ {
		key := ""
		value := ""
		result[key] = value
	}
}
func DeserializeSlice(serializedData string) {
	for i := 0; i < len(serializedData); i++ {

	}

}

func value(value interface{}) string {
	switch v := value.(type) {
	case string:
		return fmt.Sprintf("'%s'", v)
	case []interface{}: // Serialize s
		return fmt.Sprintf("'%v'", v[0])
	case int64:
		return strconv.FormatInt(v, 10)
	case float64:
		return strconv.FormatFloat(v, 'E', -1, 64)
	default:
		return fmt.Sprintf("%v", v)
	}
}

func (table *Table) GetString(keyName interface{}, key interface{}, column string) (string, bool, error) {
	var result string
	err, found := table.Get(keyName, key, []string{column}, []interface{}{&result})
	if err != nil {
		return "", found, err
	}
	return result, found, nil
}
func (table *Table) GetInt(keyName interface{}, key interface{}, column string) (int, bool, error) {
	var result int
	err, found := table.Get(keyName, key, []string{column}, []interface{}{&result})
	if err != nil {
		return 0, found, err
	}
	return result, found, nil
}
func (table *Table) GetInt64(keyName interface{}, key interface{}, column string) (int64, bool, error) {
	var result int64
	err, found := table.Get(keyName, key, []string{column}, []interface{}{&result})
	if err != nil {
		return 0, found, err
	}
	return result, found, nil
}
func (table *Table) GetFloat(keyName interface{}, key interface{}, column string) (float64, bool, error) {
	var result float64
	err, found := table.Get(keyName, key, []string{column}, []interface{}{&result})
	if err != nil {
		return 0, found, err
	}
	return result, found, nil
}
func (table *Table) GetUint64(keyName interface{}, key interface{}, column string) (uint64, bool, error) {
	var result uint64
	err, found := table.Get(keyName, key, []string{column}, []interface{}{&result})
	if err != nil {
		return 0, found, err
	}
	return result, found, nil
}
func (table *Table) GetUint(keyName interface{}, key interface{}, column string) (uint, bool, error) {
	var result uint
	err, found := table.Get(keyName, key, []string{column}, []interface{}{&result})
	if err != nil {
		return 0, found, err
	}
	return result, found, nil
}
func (table *Table) GetBoolean(keyName interface{}, key interface{}, column string) (bool, bool, error) {
	var result bool
	err, found := table.Get(keyName, key, []string{column}, []interface{}{&result})
	if err != nil {
		return false, found, err
	}
	return result, found, nil
}

func (table *Table) Get(keyName interface{}, key interface{}, columns []string, data []interface{}) (error, bool) {
	query := fmt.Sprintf("SELECT %s FROM `%s` WHERE `%v` = %s;", columnSliceToString(columns...), table.name, keyName, value(key))
	rows, err := table.Driver.Query(query)
	if err != nil {
		return err, false
	}
	if rows.Next() {
		err := rows.Scan(data...)
		if err != nil {
			return err, true
		}
		rows.Close()
	} else {
		rows.Close()
		return nil, false
	}
	return nil, true
}
func (table *Table) Put(columns []string, values []interface{}) error {
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
	query := fmt.Sprintf("INSERT INTO `%s` (%s) values (%s);", table.name, columnsString, valuesString)
	_, err := table.Driver.Exec(query)
	if err != nil {
		return err
	}
	return nil
}
func (table *Table) Set(keyName interface{}, key interface{}, columns []string, values []interface{}) error {
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
	query := fmt.Sprintf("UPDATE `%s` SET %s WHERE `%s` = %s;", table.name, s, keyName, value(key))
	_, err := table.Exec(query)
	if err != nil {
		return err
	}
	return nil
}
func (table *Table) Remove(keyName interface{}, key interface{}) error {
	query := fmt.Sprintf("DELETE FROM `%s` WHERE `%s` = %s;", table.name, keyName, value(key))
	_, err := table.Exec(query)
	if err != nil {
		return err
	}
	return nil
}

func (table *Table) Exec(query string, args ...any) (sql.Result, error) {
	query = strings.ReplaceAll(query, "{table_name}", fmt.Sprintf("`%s`", table.name))
	return table.Driver.Exec(query, args...)
}

func (table *Table) ExecSlice(query ...string) (sql.Result, error) {
	finalQuery := strings.Join(query, "\n")
	finalQuery = strings.ReplaceAll(finalQuery, "{table_name}", fmt.Sprintf("`%s`", table.name))
	log.InfoLogger.Println(finalQuery)
	return table.Driver.Exec(finalQuery)
}

func (table *Table) Query(query string, args ...any) (*sql.Rows, error) {
	query = strings.ReplaceAll(query, "{table_name}", fmt.Sprintf("`%s`", table.name))
	return table.Driver.Query(query, args...)
}

func (table *Table) ReleaseRows(rows *sql.Rows) {
	err := rows.Close()
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return
	}
}

func (table *Table) Begin() (*Tx, error) {
	driver, err := table.Driver.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{
		table:  table,
		driver: driver,
	}, nil
}

func (table *Table) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return table.Driver.BeginTx(ctx, opts)
}

func (table *Table) ExecTx(query ...string) error {
	tx, err := table.Driver.Begin()
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

func (table *Table) SingleSet(keyName string, key interface{}, column string, value interface{}) error {
	return table.Set(keyName, key, []string{column}, []interface{}{value})
}
