package main

import (
	"fmt"
	"main/cfg"
	"main/database"
	"main/log"
	"math/rand"
	"time"
)

type Status int
type PaymentStatus int

var (
	YES Status = 1
	NO  Status = 0
)

var (
	SUCCESS                Status = 100
	INVALID_REQUEST        Status = 101
	INTERNAL_SERVER_ERROR  Status = 102
	AUTHORIZATION_FAILED   Status = 103
	INVALID_CURRENCY_CODE  Status = 104
	CURRENCY_CODE_MISMATCH Status = 105
	NOT_FOUND              Status = 106
	WRONG_AMOUNT           Status = 107
	INSUFFICIENT_FUNDS     Status = 108
)

var (
	PAYMENT_SUCCESS    PaymentStatus = 0
	PAYMENT_PROCESSING PaymentStatus = 1
	PAYMENT_FAILED     PaymentStatus = 2
)

type Payment struct {
	sender   uint64
	receiver uint64
	amount   uint64
	currency int
	status   PaymentStatus
}

type Account struct {
	id       uint64
	balance  uint64
	currency int
}

/*
rows, err := database.Accounts.Query(fmt.Sprintf("SELECT @success := IF(`balance` >= %d, 1, 0) FROM {table_name} WHERE `id` = %d;", amount, sender))
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return INTERNAL_SERVER_ERROR
	}
	var success bool
	if rows.Next() {
		err = rows.Scan(&success)
		if err != nil {
			log.ErrorLogger.Println(err.Error())
			return INTERNAL_SERVER_ERROR
		}
	} else {
		return NOT_FOUND
	}
*/

func getUsername(accountId uint64) (string, bool, error) {
	return database.Accounts.GetString("id", accountId, "name")
}

func sendPayment(senderId uint64, receiverId uint64, amount uint64) Status {
	tx, err := database.Accounts.Driver.Begin()
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return INTERNAL_SERVER_ERROR
	}
	var sender Account
	var receiver Account
	sender.id = senderId
	receiver.id = receiverId

	rows, err := tx.Query(fmt.Sprintf("SELECT `balance`, `currency_code` FROM `accounts` WHERE `id` = %d;", senderId))
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return INTERNAL_SERVER_ERROR
	}
	if rows.Next() {
		err = rows.Scan(&sender.balance, &sender.currency)
		if err != nil {
			log.ErrorLogger.Println(err.Error())
			return INTERNAL_SERVER_ERROR
		}
	} else {
		return NOT_FOUND
	}
	err = rows.Close()
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return INTERNAL_SERVER_ERROR
	}

	rows, err = tx.Query(fmt.Sprintf("SELECT `balance`, `currency_code` FROM `accounts` WHERE `id` = %d;", receiverId))
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return INTERNAL_SERVER_ERROR
	}
	if rows.Next() {
		err = rows.Scan(&receiver.balance, &receiver.currency)
		if err != nil {
			log.ErrorLogger.Println(err.Error())
			return INTERNAL_SERVER_ERROR
		}
	} else {
		return NOT_FOUND
	}
	err = rows.Close()
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return INTERNAL_SERVER_ERROR
	}

	if sender.currency != receiver.currency {
		return CURRENCY_CODE_MISMATCH
	}
	if sender.balance < amount {
		return INSUFFICIENT_FUNDS
	}

	_, err = tx.Exec(fmt.Sprintf("UPDATE `accounts` SET `balance` = `balance` - %d WHERE `id` = %d;", amount, senderId))
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return INTERNAL_SERVER_ERROR
	}
	_, err = tx.Exec(fmt.Sprintf("UPDATE `accounts` SET `balance` = `balance` - %d WHERE `id` = %d;", amount, receiverId))
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return INTERNAL_SERVER_ERROR
	}

	err = tx.Commit()
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return INTERNAL_SERVER_ERROR
	}
	return SUCCESS
}

func getBalance(accountId uint64) (uint64, Status) {
	balance, found, err := database.Accounts.GetUint64("id", accountId, "balance")
	if err != nil {
		return 0, INTERNAL_SERVER_ERROR
	}
	if !found {
		return 0, NOT_FOUND
	}
	return balance, SUCCESS
}

func getHistory(accountId uint64) ([]Payment, Status) {
	database.Accounts.GetUint64("id", accountId, "balance")
	return nil, 0
}

func getAccountInfo(accountId uint64) (*Account, Status) {
	rows, err := database.Accounts.Query(fmt.Sprintf("SELECT `balance`, `currency_code` FROM {table_name} WHERE `id` = %d;", accountId))
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return nil, INTERNAL_SERVER_ERROR
	}
	var account Account
	account.id = accountId
	if rows.Next() {
		err = rows.Scan(&account.balance, &account.currency)
		if err != nil {
			log.ErrorLogger.Println(err.Error())
			return nil, INTERNAL_SERVER_ERROR
		}
	} else {
		return nil, NOT_FOUND
	}
	return &account, 0
}

func panicIfError(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	rand.Seed(time.Now().UnixNano())

	log.InfoLogger.Println("Starting...")
	log.InfoLogger.Println("Config loading")
	panicIfError(cfg.LoadConfig())
	log.InfoLogger.Println("Database start")
	panicIfError(database.InitDatabase())

	log.InfoLogger.Println("Fiber initializing")
	initFiber()

	log.InfoLogger.Println("Fiber run")
	panicIfError(runFiber())

	log.InfoLogger.Println("Shutdown...")
	log.InfoLogger.Println("Goodbye!")

}
