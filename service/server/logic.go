package server

import (
	"main/accounts"
	"main/database"
	"main/log"
	"main/payments"
	"main/status"
	"strconv"
)

func getUsername(accountId uint64) (string, bool, error) {
	return database.Accounts.GetString("id", accountId, "name")
}

func sendPayment(senderId uint64, receiverId uint64, amount uint64, timestamp uint64) status.Status {
	// check on currency code mismatch
	sender, err := accounts.Load(senderId)
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return status.INTERNAL_SERVER_ERROR
	}
	if sender == nil {
		return status.NOT_FOUND
	}
	receiver, err := accounts.Load(receiverId)
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return status.INTERNAL_SERVER_ERROR
	}
	if receiver == nil {
		return status.NOT_FOUND
	}

	if sender.Currency != receiver.Currency {
		return status.CURRENCY_CODE_MISMATCH
	}
	// start transaction
	tx, err := database.LastSerial.Begin(senderId)
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return status.INTERNAL_SERVER_ERROR
	}

	balance, err := sender.GetBalance()
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		tx.Fail()
		return status.INTERNAL_SERVER_ERROR
	}
	if balance < amount {
		tx.Fail()
		return status.INSUFFICIENT_FUNDS
	}
	payment := payments.New(sender, receiver, amount, timestamp)

	err = payment.Commit(tx)
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		tx.Fail()
		return status.INTERNAL_SERVER_ERROR
	}
	go func() {
		err := database.Balances.Set(senderId, []string{"onSerial", "balance"}, []interface{}{payment.Timestamp, balance - amount})
		if err != nil {
			log.ErrorLogger.Println(err.Error())
			return
		}
		err = database.Balances.Set(receiverId, []string{"onSerial", "balance"}, []interface{}{payment.Timestamp, balance + amount})
		if err != nil {
			log.ErrorLogger.Println(err.Error())
			return
		}
	}()

	return status.SUCCESS
}

func getBalance(accountId uint64) (status.Status, uint64) {
	account, err := accounts.Load(accountId)
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return status.INTERNAL_SERVER_ERROR, 0
	}
	if account == nil {
		return status.NOT_FOUND, 0
	}
	balance, err := account.GetBalance()
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return status.INTERNAL_SERVER_ERROR, 0
	}
	return status.SUCCESS, balance
}

func getHistory(accountId uint64) (status.Status, []uint64) {
	database.Accounts.GetUint64("id", accountId, "balance")
	return 0, nil
}

func getAccountInfo(accountId uint64) (status.Status, *accounts.Account) {
	account, err := accounts.Load(accountId)
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return status.INTERNAL_SERVER_ERROR, nil
	}
	return status.SUCCESS, account
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
	err := database.Accounts.Put("id", accountId, []string{"name", "currency"}, []interface{}{username, currency})
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return status.INTERNAL_SERVER_ERROR, 0
	}
	err = database.Balances.Put(accountId, []string{"balance"}, []interface{}{1000000})
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return status.INTERNAL_SERVER_ERROR, 0
	}
	err = database.LastSerial.Put(accountId, []string{"lastSerial"}, []interface{}{timestamp})
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return status.INTERNAL_SERVER_ERROR, 0
	}
	return status.SUCCESS, accountId
}

/*
tx, err := database.Accounts.RawTx(senderId)
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
*/
