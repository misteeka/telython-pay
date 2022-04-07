package server

import (
	"fmt"
	"main/account"
	"main/database"
	"main/log"
	"main/payment"
	"main/status"
	"strconv"
)

func getUsername(accountId uint64) (string, bool, error) {
	return database.Accounts.GetString("id", accountId, "name")
}

func sendPayment(senderId uint64, receiverId uint64, amount uint64, timestamp uint64) status.Status {
	tx, err := database.Accounts.Driver.Begin()
	if err != nil {
		tx.Rollback()
		log.ErrorLogger.Println(err.Error())
		return status.INTERNAL_SERVER_ERROR
	}

	sender, err := account.Load(senderId, tx)
	if err != nil {
		tx.Rollback()
		log.ErrorLogger.Println(err.Error())
		return status.INTERNAL_SERVER_ERROR
	}
	if sender == nil {
		tx.Rollback()
		return status.NOT_FOUND
	}
	receiver, err := account.Load(receiverId, tx)
	if err != nil {
		tx.Rollback()
		log.ErrorLogger.Println(err.Error())
		return status.INTERNAL_SERVER_ERROR
	}
	if receiver == nil {
		tx.Rollback()
		return status.NOT_FOUND
	}

	if sender.Currency != receiver.Currency {
		return status.CURRENCY_CODE_MISMATCH
	}
	if sender.Balance < amount {
		return status.INSUFFICIENT_FUNDS
	}
	payment := payment.New(sender, receiver, amount, tx, timestamp)
	err = payment.Transfer()
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		payment.Fail()
		return status.INTERNAL_SERVER_ERROR
	}
	err = payment.Commit()
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		payment.Fail()
		return status.INTERNAL_SERVER_ERROR
	}
	return status.SUCCESS
}

func getBalance(accountId uint64) (status.Status, uint64) {
	balance, found, err := database.Accounts.GetUint64("id", accountId, "balance")
	if err != nil {
		return status.INTERNAL_SERVER_ERROR, 0
	}
	if !found {
		return status.NOT_FOUND, 0
	}
	return status.SUCCESS, balance
}

func getHistory(accountId uint64) (status.Status, []payment.Payment) {
	database.Accounts.GetUint64("id", accountId, "balance")
	return 0, nil
}

func getAccountInfo(accountId uint64) (status.Status, *account.Account) {
	rows, err := database.Accounts.Query(fmt.Sprintf("SELECT `name`, `balance`, `currency` FROM {table_name} WHERE `id` = %d;", accountId))
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return status.INTERNAL_SERVER_ERROR, nil
	}
	var account account.Account
	account.Id = accountId
	if rows.Next() {
		err = rows.Scan(&account.Username, &account.Balance, &account.Currency)
		if err != nil {
			log.ErrorLogger.Println(err.Error())
			return status.INTERNAL_SERVER_ERROR, nil
		}
	} else {
		return status.NOT_FOUND, nil
	}
	return status.SUCCESS, &account
}

func fnv64(key string) uint64 {
	hash := uint64(4332272522)
	const prime64 = uint64(33555238)
	keyLength := len(key)
	for i := 0; i < keyLength; i++ {
		hash *= prime64
		hash ^= uint64(key[i])
	}
	return hash
}

func createAccount(username string, currency int, timestamp uint64) (status.Status, uint64) {
	accountId := fnv64(username + strconv.FormatUint(timestamp, 10))
	err := database.Accounts.Put([]string{"id", "name", "balance", "currency"}, []interface{}{accountId, username, 1000000, currency})
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return status.INTERNAL_SERVER_ERROR, 0
	}
	return status.SUCCESS, accountId
}
