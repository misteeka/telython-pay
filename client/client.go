package main

import (
	"bufio"
	"fmt"
	transport "github.com/misteeka/fasthttp"
	"github.com/valyala/fastjson"
	"os"
	"strconv"
	"strings"
)

type Response interface{}
type Status int

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
	TOO_MANY_REQUESTS      Status = 109
)

func statusToString(status Status) string {
	if status == SUCCESS {
		return "SUCCESS"
	}
	if status == INVALID_REQUEST {
		return "INVALID REQUEST"
	}
	if status == INTERNAL_SERVER_ERROR {
		return "INTERNAL SERVER ERROR"
	}
	if status == AUTHORIZATION_FAILED {
		return "AUTHORIZATION FAILED"
	}
	if status == INVALID_CURRENCY_CODE {
		return "INVALID CURRENCY CODE"
	}
	if status == CURRENCY_CODE_MISMATCH {
		return "CURRENCY CODE MISMATCH"
	}
	if status == WRONG_AMOUNT {
		return "WRONG AMOUNT"
	}
	if status == NOT_FOUND {
		return "NOT FOUND"
	}
	if status == INSUFFICIENT_FUNDS {
		return "INSUFFICIENT FUNDS"
	}
	if status == TOO_MANY_REQUESTS {
		return "TOO MANY REQUESTS"
	}
	return fmt.Sprintf("%v", status)

}

const ip = "127.0.0.1" // 192.168.1.237

func get(function string) ([]byte, error) {
	resp, err := transport.Get(fmt.Sprintf("http://%s:8002/payments/%s", ip, function))
	if err != nil {
		return nil, err
	}
	response := resp.Body()
	transport.ReleaseResponse(resp)
	return response, nil
}
func post(function string, data string) ([]byte, error) {
	resp, err := transport.Post(fmt.Sprintf("http://%s:8002/payments/%s", ip, function), []byte(data))
	if err != nil {
		return nil, err
	}
	response := resp.Body()
	transport.ReleaseResponse(resp)
	return response, nil
}
func put(function string, data string) ([]byte, error) {
	resp, err := transport.Put(fmt.Sprintf("http://%s:8002/payments/%s", ip, function), []byte(data))
	if err != nil {
		return nil, err
	}
	response := resp.Body()
	transport.ReleaseResponse(resp)
	return response, nil
}

func getStatus(value *fastjson.Value) Status {
	return Status(value.GetInt("status"))
}

func SendPayment(sender uint64, receiver uint64, amount uint64, password string) (Status, error) {
	body, err := post("sendPayment", fmt.Sprintf(`{"sender":%d,"receiver":%d,"amount":%d, "password":"%s"}`, sender, receiver, amount, password))
	if err != nil {
		return 0, err
	}
	var p fastjson.Parser
	json, err := p.ParseBytes(body)
	if err != nil {
		return 0, err
	}
	return getStatus(json), nil
}

func GetBalance(accountId uint64, password string) (uint64, Status, error) {
	body, err := get("getBalance?a=" + strconv.FormatUint(accountId, 10) + "&p=" + password)
	if err != nil {
		return 0, 0, err
	}
	var p fastjson.Parser
	json, err := p.ParseBytes(body)
	if err != nil {
		return 0, 0, err
	}
	return json.GetUint64("data"), getStatus(json), nil
}

func CreateAccount(username string, password string, currency int) (uint64, Status, error) {
	body, err := post("createAccount", fmt.Sprintf(`{"username":"%s","password":"%s","currency":%d}`, username, password, currency))
	if err != nil {
		return 0, 0, err
	}
	var p fastjson.Parser
	json, err := p.ParseBytes(body)
	if err != nil {
		return 0, 0, err
	}
	return json.GetUint64("data"), getStatus(json), nil
}

func print(data interface{}, status Status, err error) {
	if err != nil {
		fmt.Println("ERR: " + err.Error())
		return
	}
	if data == nil {
		fmt.Println(fmt.Sprintf("Status: %s", statusToString(status)))
	} else {
		fmt.Println(fmt.Sprintf("Status: %s\nData: %v", statusToString(status), data))
	}
}

func main() {
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Telython Pay Shell")
	/*var wg sync.WaitGroup
	for a := 0; a < 100; a++ {
		wg.Add(1)
		time.Sleep(time.Microsecond)
		go func() {
			rand.Seed(time.Now().UnixNano())
			id := strconv.Itoa(rand.Intn(10000000))
			sender, status, err := CreateAccount(id, id, 0)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			fmt.Println(statusToString(status))
			receiver, status, err := CreateAccount(id, id, 0)
			if err != nil {
				fmt.Println(err.Error())
				return
			}
			fmt.Println(statusToString(status))
			start := time.Now()
			for i := 0; i < 100; i++ {
				SendPayment(sender, receiver, 1, id)
			}
			fmt.Println(statusToString(status), time.Now().Sub(start).Milliseconds(), "ms")
			wg.Done()
		}()
	}
	wg.Wait()
	log.Println("Done")
	return*/

	fmt.Println("---------------------")
	for {
		fmt.Print("-> ")
		text, _ := reader.ReadString('\n')
		text = strings.ReplaceAll(text, "\n", "")
		text = strings.ReplaceAll(text, "\r", "")
		parts := strings.Split(text, " ")
		if len(parts) < 1 {
			fmt.Println("Wrong command")
			continue
		}
		cmd := parts[0]
		args := parts[1:]
		if strings.Compare("getBalance", cmd) == 0 {
			if len(args) < 2 {
				fmt.Println("Wrong args")
				continue
			}
			id, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			password := args[1]
			balance, status, err := GetBalance(id, password)
			print(balance, status, err)
		} else if strings.Compare("createAccount", cmd) == 0 {
			if len(args) < 3 {
				fmt.Println("Wrong args")
				continue
			}
			username := args[0]
			password := args[1]
			currency, err := strconv.Atoi(args[2])
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			data, status, err := CreateAccount(username, password, currency)
			print(data, status, err)
		} else if strings.Compare("sendPayment", cmd) == 0 {
			if len(args) < 4 {
				fmt.Println("Wrong args")
				continue
			}
			sender, err := strconv.ParseUint(args[0], 10, 64)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			receiver, err := strconv.ParseUint(args[1], 10, 64)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			amount, err := strconv.ParseUint(args[2], 10, 64)
			if err != nil {
				fmt.Println(err.Error())
				continue
			}
			password := args[3]
			status, err := SendPayment(sender, receiver, amount, password)
			print(nil, status, err)
		} else {
			fmt.Println("Unknown command.")
		}
	}

}
