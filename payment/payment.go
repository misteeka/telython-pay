package payment

import (
	"database/sql"
	"fmt"
	"main/account"
	"main/database"
	"strconv"
	"time"
)

type Payment struct {
	Id       uint64
	Sender   uint64
	Receiver uint64
	Amount   uint64
	Currency int
	Status   Status
	Tx       *sql.Tx
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

func New(sender *account.Account, receiver *account.Account, amount uint64, tx *sql.Tx) (*Payment, error) {
	payment := Payment{
		Id:       fnv64(strconv.FormatUint(sender.Id, 10) + strconv.FormatUint(receiver.Id, 10) + strconv.FormatInt(time.Now().UnixMicro(), 10)),
		Sender:   sender.Id,
		Receiver: receiver.Id,
		Amount:   amount,
		Currency: sender.Currency,
		Status:   PROCESSING,
		Tx:       tx,
	}
	err := database.Payments.Put([]string{"id", "sender", "receiver", "amount", "currency", "status"},
		[]interface{}{payment.Id, payment.Sender, payment.Receiver, payment.Amount, payment.Currency, payment.Status})
	if err != nil {
		return nil, err
	}
	return &payment, nil
}

func (payment *Payment) SetStatus(status Status) error {
	payment.Status = status
	return database.Payments.SingleSet("id", payment.Id, "status", status)
}

func (payment *Payment) Transfer() error {
	_, err := payment.Tx.Exec(fmt.Sprintf("UPDATE `accounts` SET `balance` = `balance` - %d WHERE `id` = %d;", payment.Amount, payment.Sender))
	if err != nil {
		return err
	}
	_, err = payment.Tx.Exec(fmt.Sprintf("UPDATE `accounts` SET `balance` = `balance` + %d WHERE `id` = %d;", payment.Amount, payment.Sender))
	if err != nil {
		return err
	}
	return nil
}

func (payment *Payment) Commit() error {
	err := payment.SetStatus(SUCCESS)
	if err != nil {
		return err
	}
	return payment.Tx.Commit()
}

func (payment *Payment) Fail() {
	payment.SetStatus(FAILED)
	payment.Tx.Rollback()
}

func (payment *Payment) Rollback() error {
	return payment.Tx.Rollback()
}
