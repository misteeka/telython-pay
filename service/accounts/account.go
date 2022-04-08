package accounts

import (
	"database/sql"
	"fmt"
	"main/database"
)

type Account struct {
	Id       uint64
	Username string
	Currency int
	Balance  uint64
}

func Load(id uint64, tx *sql.Tx, lock bool) (*Account, error) {
	var lockStatement string
	if lock {
		lockStatement = " FOR UPDATE;"
	} else {
		lockStatement = ""
	}
	if tx == nil {
		rows, err := database.Accounts.Query(fmt.Sprintf("SELECT `name`, `balance`, `currency` FROM {table} WHERE `id` = %d%s;", id, lockStatement), id)
		if err != nil {
			return nil, err
		}
		var account Account
		account.Id = id
		if rows.Next() {
			err = rows.Scan(&account.Username, &account.Balance, &account.Currency)
			if err != nil {
				rows.Close()
				return nil, err
			}
		} else {
			rows.Close()
			return nil, nil
		}
		err = rows.Close()
		return &account, err
	} else {
		rows, err := tx.Query(fmt.Sprintf("SELECT `name`, `balance`, `currency` FROM {table} WHERE `id` = %d%s;", id, lockStatement))
		if err != nil {
			return nil, err
		}
		var account Account
		account.Id = id
		if rows.Next() {
			err = rows.Scan(&account.Username, &account.Balance, &account.Currency)
			if err != nil {
				rows.Close()
				return nil, err
			}
		} else {
			rows.Close()
			return nil, nil
		}
		err = rows.Close()
		return &account, err
	}
}
