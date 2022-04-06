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

var db *sql.DB

var (
	Accounts *eplidr.Table
	Payments *eplidr.Table
)

func InitDatabase() error {
	var err error
	dataSource := "{user}:{password}@tcp(localhost:41091)/{db}"
	dataSource = strings.Replace(dataSource, "{user}", cfg.GetString("user"), 1)
	dataSource = strings.Replace(dataSource, "{password}", cfg.GetString("password"), 1)
	dataSource = strings.Replace(dataSource, "{db}", cfg.GetString("dbName"), 1)
	db, err = sql.Open("mysql", dataSource)
	if err != nil {
		return err
	}
	db.SetConnMaxLifetime(2 * time.Minute)
	db.SetConnMaxIdleTime(2 * time.Minute)
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(10)

	//Users = eplidr.NewKeyTable("users", db)
	//SingleUsers = eplidr.SingleKeyImplementation(Users, "name")
	//EmailCodes = eplidr.NewSingleKeyTable("emailcodes", "name", db)
	Payments = eplidr.NewKeyTable("payments", db)
	Accounts = eplidr.NewKeyTable("accounts", db)
	return nil
}
func ReleaseRows(rows *sql.Rows) {
	err := rows.Close()
	if err != nil {
		log.ErrorLogger.Print(err.Error())
		return
	}
}
