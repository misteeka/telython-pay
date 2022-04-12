package payments

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
)

type Payment struct {
	Id        uint64
	Sender    uint64
	Receiver  uint64
	Amount    uint64
	Timestamp uint64
	Currency  uint64
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

func New(sender uint64, receiver uint64, amount uint64, timestamp uint64) *Payment {
	return nil
}

func (payment *Payment) Commit() error {
	return nil
}

func (payment *Payment) Serialize() ([]byte, error) {
	buff := new(bytes.Buffer)
	err := binary.Write(buff, binary.BigEndian, payment.Id)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buff, binary.BigEndian, payment.Sender)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buff, binary.BigEndian, payment.Receiver)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buff, binary.BigEndian, payment.Amount)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buff, binary.BigEndian, payment.Timestamp)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buff, binary.BigEndian, payment.Currency)
	if err != nil {
		return nil, err
	}

	return buff.Bytes(), nil
}

func (payment Payment) SerializeReadable() ([]byte, error) {
	jsonData, err := json.Marshal(payment)
	if err != nil {
		return nil, err
	}
	return jsonData, nil
}

func DeserializePayment(serialized []byte) Payment {
	return Payment{
		Id:        binary.BigEndian.Uint64(serialized[0:8]),
		Sender:    binary.BigEndian.Uint64(serialized[8:16]),
		Receiver:  binary.BigEndian.Uint64(serialized[16:24]),
		Amount:    binary.BigEndian.Uint64(serialized[24:32]),
		Timestamp: binary.BigEndian.Uint64(serialized[32:40]),
		Currency:  binary.BigEndian.Uint64(serialized[40:48]),
	}
}

func SerializePayments(payments []Payment) ([]byte, error) {
	buff := new(bytes.Buffer)
	for i := 0; i < len(payments); i++ {
		serialized, err := payments[i].Serialize()
		if err != nil {
			return nil, err
		}
		buff.Write(serialized)
	}
	return buff.Bytes(), nil
}

func DeserializePayments(serialized []byte) *[]Payment {
	var payments []Payment
	for i := 0; i < (len(serialized) / 48); i++ {
		payments = append(payments, DeserializePayment(serialized[i*48:(i+1)*48]))
	}
	return &payments
}
