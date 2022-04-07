package database

import (
	"database/sql"
	_ "github.com/go-sql-driver/mysql"
	"main/cfg"
	"main/database/eplidr"
	"main/log"
	"strings"
	"time"
)

var (
	Accounts   *eplidr.Table
	Payments   *eplidr.Table
	LastActive *eplidr.SingleKeyTable
)

func InitDatabase() error {
	var err error
	dataSource := "{user}:{password}@tcp(localhost:41091)/{db}"
	dataSource = strings.Replace(dataSource, "{user}", cfg.GetString("user"), 1)
	dataSource = strings.Replace(dataSource, "{password}", cfg.GetString("password"), 1)
	dataSource = strings.Replace(dataSource, "{db}", cfg.GetString("dbName"), 1)
	defaultDriver, err := sql.Open("mysql", dataSource)
	if err != nil {
		return err
	}
	defaultDriver.SetConnMaxLifetime(1 * time.Minute)
	defaultDriver.SetConnMaxIdleTime(30 * time.Second)
	defaultDriver.SetMaxIdleConns(cfg.GetInt("maxIdleConns"))
	defaultDriver.SetMaxOpenConns(cfg.GetInt("maxOpenConns"))

	Payments = eplidr.NewTable("payments", defaultDriver)
	Accounts = eplidr.NewTable("accounts", defaultDriver)
	LastActive = eplidr.NewSingleKeyTable("lastactive", "name", defaultDriver)
	return nil
}
func ReleaseRows(rows *sql.Rows) {
	err := rows.Close()
	if err != nil {
		log.ErrorLogger.Print(err.Error())
		return
	}
}
