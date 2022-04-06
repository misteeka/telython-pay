package account

import (
	"database/sql"
	"fmt"
)

type Account struct {
	Id       uint64
	Username string
	Currency int
	Balance  uint64
}

func Load(id uint64, tx *sql.Tx) (*Account, error) {
	rows, err := tx.Query(fmt.Sprintf("SELECT `name`, `balance`, `currency` FROM `accounts` WHERE `id` = %d;", id))
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
