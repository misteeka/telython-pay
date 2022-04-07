package payments

import (
	"database/sql"
	"fmt"
	"main/database"
	"main/log"
	"strconv"
)

type Payment struct {
	Id        uint64
	Sender    uint64
	Receiver  uint64
	Amount    uint64
	Timestamp uint64
	Currency  int
	Tx        *sql.Tx
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

func New(senderId uint64, receiverId uint64, amount uint64, currency int, timestamp uint64) *Payment {
	payment := Payment{
		Id:        fnv64(strconv.FormatUint(senderId, 10) + strconv.FormatUint(receiverId, 10) + strconv.FormatUint(timestamp, 10)),
		Sender:    senderId,
		Receiver:  receiverId,
		Amount:    amount,
		Currency:  currency,
		Timestamp: timestamp,
	}
	return &payment
}

func (payment *Payment) Transfer() error {
	_, err := payment.Tx.Exec(fmt.Sprintf("UPDATE `accounts` SET `balance` = `balance` - %d WHERE `id` = %d;", payment.Amount, payment.Sender))
	if err != nil {
		return err
	}
	_, err = payment.Tx.Exec(fmt.Sprintf("UPDATE `accounts` SET `balance` = `balance` + %d WHERE `id` = %d;", payment.Amount, payment.Receiver))
	if err != nil {
		return err
	}
	return nil
}

func (payment *Payment) Commit() error {
	err := payment.Tx.Commit()
	if err != nil {
		return err
	}
	return database.Payments.Put("id", payment.Id, []string{"sender", "receiver", "amount", "currency", "status", "timestamp"},
		[]interface{}{payment.Sender, payment.Receiver, payment.Amount, payment.Currency, SUCCESS, payment.Timestamp})
}

func (payment *Payment) Fail() {
	err := payment.Tx.Rollback()
	if err != nil {
		log.ErrorLogger.Println(err.Error())
	}
	err = database.Payments.Put("id", payment.Id, []string{"sender", "receiver", "amount", "currency", "status", "timestamp"},
		[]interface{}{payment.Sender, payment.Receiver, payment.Amount, payment.Currency, FAILED, payment.Timestamp})
	if err != nil {
		log.ErrorLogger.Println(err.Error())
	}
}

func (payment *Payment) Rollback() error {
	err := payment.Tx.Rollback()
	if err != nil {
		return err
	}
	return database.Payments.Put("id", payment.Id, []string{"sender", "receiver", "amount", "currency", "status", "timestamp"},
		[]interface{}{payment.Sender, payment.Receiver, payment.Amount, payment.Currency, FAILED, payment.Timestamp})
}
