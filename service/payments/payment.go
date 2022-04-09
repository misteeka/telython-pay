package payments

import (
	"main/accounts"
	"main/database/eplidr"
	"strconv"
)

type Payment struct {
	Id        uint64
	Sender    *accounts.Account
	Receiver  *accounts.Account
	Amount    uint64
	Timestamp uint64
	Currency  int
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

func New(sender *accounts.Account, receiver *accounts.Account, amount uint64, timestamp uint64) *Payment {
	payment := Payment{
		Id:        fnv64(strconv.FormatUint(sender.Id, 10) + strconv.FormatUint(receiver.Id, 10) + strconv.FormatUint(timestamp, 10)),
		Sender:    sender,
		Receiver:  receiver,
		Amount:    amount,
		Currency:  sender.Currency,
		Timestamp: timestamp,
	}
	return &payment
}

func (payment *Payment) Commit(tx *eplidr.Tx) error {
	err := tx.Put("payments",
		[]string{"id", "sender", "receiver", "amount", "timestamp", "currency"},
		[]interface{}{payment.Id, payment.Sender.Id, payment.Receiver.Id, payment.Amount, payment.Timestamp, payment.Currency})
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}
	return nil
}
