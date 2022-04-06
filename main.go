package main

import (
	"fmt"
	"main/account"
	"main/cfg"
	"main/database"
	"main/log"
	"main/payment"
	"math/rand"
	"strconv"
	"time"
)

type Status int

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
		tx.Rollback()
		log.ErrorLogger.Println(err.Error())
		return INTERNAL_SERVER_ERROR
	}

	sender, err := account.Load(senderId, tx)
	if err != nil {
		tx.Rollback()
		log.ErrorLogger.Println(err.Error())
		return INTERNAL_SERVER_ERROR
	}
	if sender == nil {
		tx.Rollback()
		return NOT_FOUND
	}
	receiver, err := account.Load(receiverId, tx)
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return INTERNAL_SERVER_ERROR
	}
	if receiver == nil {
		tx.Rollback()
		return NOT_FOUND
	}

	if sender.Currency != receiver.Currency {
		return CURRENCY_CODE_MISMATCH
	}
	if sender.Balance < amount {
		return INSUFFICIENT_FUNDS
	}

	payment, err := payment.New(sender, receiver, amount, tx)
	if err != nil {
		payment.Fail()
		return INTERNAL_SERVER_ERROR
	}
	err = payment.Transfer()
	if err != nil {
		return INTERNAL_SERVER_ERROR
	}
	err = payment.Commit()
	if err != nil {
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

func getHistory(accountId uint64) ([]payment.Payment, Status) {
	database.Accounts.GetUint64("id", accountId, "balance")
	return nil, 0
}

func getAccountInfo(accountId uint64) (*account.Account, Status) {
	rows, err := database.Accounts.Query(fmt.Sprintf("SELECT `name`, `balance`, `currency` FROM {table_name} WHERE `id` = %d;", accountId))
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return nil, INTERNAL_SERVER_ERROR
	}
	var account account.Account
	account.Id = accountId
	if rows.Next() {
		err = rows.Scan(&account.Username, &account.Balance, &account.Currency)
		if err != nil {
			log.ErrorLogger.Println(err.Error())
			return nil, INTERNAL_SERVER_ERROR
		}
	} else {
		return nil, NOT_FOUND
	}
	return &account, 0
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

func createAccount(username string, currency int) (accountId uint64, status Status) {
	accountId = fnv64(username + strconv.FormatInt(time.Now().UnixMicro(), 10))
	err := database.Accounts.Put([]string{"id", "name", "balance", "currency"}, []interface{}{accountId, username, 0, currency})
	if err != nil {
		return 0, INTERNAL_SERVER_ERROR
	}
	status = SUCCESS
	return
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
