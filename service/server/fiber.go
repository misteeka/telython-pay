package server

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"github.com/valyala/fastjson"
	"main/log"
	"main/status"
	"strconv"
	"time"
)

var App *fiber.App

type Handler interface{}

type ReturnDataHandler func(ctx *fiber.Ctx) (status.Status, interface{})
type DefaultHandler func(ctx *fiber.Ctx) status.Status

func RespondJson(ctx *fiber.Ctx, status status.Status, json []byte) {

}
func Respond(ctx *fiber.Ctx, status status.Status) error {
	json := fmt.Sprintf(`{"status": %d}`, status)
	_, err := ctx.WriteString(json)
	return err
}
func RespondGet(ctx *fiber.Ctx, status status.Status, resp interface{}) error {
	var json string
	switch v := resp.(type) {
	case string:
		json = fmt.Sprintf(`{"data":"%s", "status": %d}`, v, status)
	case []byte:
		json = fmt.Sprintf(`{"data":"%s", "status": %d}`, base64.StdEncoding.EncodeToString(v), status)
	case int64:
		json = fmt.Sprintf(`{"data": %d, "status": %d}`, v, status)
	case int:
		json = fmt.Sprintf(`{"data": %d, "status": %d}`, v, status)
	case uint64:
		json = fmt.Sprintf(`{"data": %d, "status": %d}`, v, status)
	case bool:
		json = fmt.Sprintf(`{"data": %t, "status": %d}`, v, status)
	default:
		json = fmt.Sprintf(`{"data": "%v", "status": %d}`, v, status)
	}
	_, err := ctx.WriteString(json)
	return err
}

func Post(path string, handler Handler) fiber.Router {
	switch handler := handler.(type) {
	case DefaultHandler:
		return App.Post(path, func(ctx *fiber.Ctx) error {
			return Respond(ctx, handler(ctx))
		})
	case ReturnDataHandler:
		return App.Post(path, func(ctx *fiber.Ctx) error {
			status, resp := handler(ctx)
			if resp == nil {
				return Respond(ctx, status)
			} else {
				return RespondGet(ctx, status, resp)
			}
		})
	default:
		return nil
	}
}
func Put(path string, handler Handler) fiber.Router {
	switch handler := handler.(type) {
	case DefaultHandler:
		return App.Put(path, func(ctx *fiber.Ctx) error {
			return Respond(ctx, handler(ctx))
		})
	case ReturnDataHandler:
		return App.Put(path, func(ctx *fiber.Ctx) error {
			status, resp := handler(ctx)
			if resp == nil {
				return Respond(ctx, status)
			} else {
				return RespondGet(ctx, status, resp)
			}
		})
	default:
		return nil
	}
}
func Delete(path string, handler Handler) fiber.Router {
	switch handler := handler.(type) {
	case DefaultHandler:
		return App.Delete(path, func(ctx *fiber.Ctx) error {
			return Respond(ctx, handler(ctx))
		})
	case ReturnDataHandler:
		return App.Delete(path, func(ctx *fiber.Ctx) error {
			status, resp := handler(ctx)
			if resp == nil {
				return Respond(ctx, status)
			} else {
				return RespondGet(ctx, status, resp)
			}
		})
	default:
		return nil
	}
}
func Get(path string, handler Handler) fiber.Router {
	switch handler := handler.(type) {
	case DefaultHandler:
		return App.Get(path, func(ctx *fiber.Ctx) error {
			return Respond(ctx, handler(ctx))
		})
	case ReturnDataHandler:
		return App.Get(path, func(ctx *fiber.Ctx) error {
			status, resp := handler(ctx)
			if resp == nil {
				return Respond(ctx, status)
			} else {
				return RespondGet(ctx, status, resp)
			}
		})
	default:
		return nil
	}
}

func Init() {
	App = fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})
	registerHandlers()
}

func Run() error {
	return App.Listen(":8002")
}

func Deserialize(jsonBytes []byte) (data *fastjson.Value, err error) {
	var p fastjson.Parser
	data, err = p.ParseBytes(jsonBytes)
	return
}

func getUniqueTimestamp(username string) (uint64, status.Status) {
	timestamp := time.Now().UnixMicro()
	/*
		tx, err := database.LastActive.Begin()
		if err != nil {
			tx.Fail()
			log.ErrorLogger.Println(err.Error())
			return 0, status.INTERNAL_SERVER_ERROR
		}
		lastActive, found, err := tx.GetUint("name", username, "lastActive")
		if err != nil {
			tx.Fail()
			log.ErrorLogger.Println(err.Error())
			return 0, status.INTERNAL_SERVER_ERROR
		}
		if !found {
			tx.Fail()
			err := database.LastActive.Put(username, []string{"lastActive"}, []interface{}{timestamp})
			if err != nil {
				log.ErrorLogger.Println(err.Error())
				return 0, status.INTERNAL_SERVER_ERROR
			}
			return 0, status.SUCCESS
		} else {
			err := tx.SingleSet("name", username, "lastActive", timestamp)
			if err != nil {
				tx.Fail()
				log.ErrorLogger.Println(err.Error())
				return 0, status.INTERNAL_SERVER_ERROR
			}
		}
		if uint64(timestamp)-lastActive < 1 {
			err = tx.Commit()
			if err != nil {
				tx.Fail()
				log.ErrorLogger.Println(err.Error())
				return 0, status.INTERNAL_SERVER_ERROR
			}
			return 0, status.TOO_MANY_REQUESTS
		}
		err = tx.Commit()
		if err != nil {
			tx.Fail()
			log.ErrorLogger.Println(err.Error())
			return 0, status.INTERNAL_SERVER_ERROR
		}*/
	return uint64(timestamp), status.SUCCESS
}

func authorize(username string, password string) status.Status {
	/*
		resp, err := auth.CheckPassword(username, password)
		if err != nil {
			log.ErrorLogger.Println(err.Error())
			return status.INTERNAL_SERVER_ERROR
		}
		if bytes.Equal(resp, auth.AUTHORIZATION_FAILED) {
			return status.AUTHORIZATION_FAILED
		} else if bytes.Equal(resp, auth.SUCCESS) {
			return status.SUCCESS
		} else {
			return status.INTERNAL_SERVER_ERROR
		}*/
	return status.SUCCESS
}

func Serialize(i interface{}) []byte {
	jsonData, err := json.Marshal(i)
	if err != nil {
		return nil
	}
	return jsonData
}

func registerHandlers() {
	Post("/payments/sendPayment", DefaultHandler(func(ctx *fiber.Ctx) status.Status {
		data, err := Deserialize(ctx.Body())
		if err != nil {
			return status.INVALID_REQUEST
		}
		sender := data.GetUint64("sender")
		receiver := data.GetUint64("receiver")
		amount := data.GetUint64("amount")
		password := string(data.GetStringBytes("password"))

		username, found, err := getUsername(sender)
		if err != nil {
			return status.INTERNAL_SERVER_ERROR
		}
		if !found {
			return status.AUTHORIZATION_FAILED
		}

		authorizationStatus := authorize(username, password)
		if authorizationStatus == status.SUCCESS {
			timestamp, timestampStatus := getUniqueTimestamp(username)
			if timestampStatus == status.SUCCESS {
				return sendPayment(sender, receiver, amount, timestamp)
			} else {
				return timestampStatus
			}
		} else {
			return authorizationStatus
		}
	}))
	Get("/payments/getAccountInfo", ReturnDataHandler(func(ctx *fiber.Ctx) (status.Status, interface{}) {
		accountId, err := strconv.ParseUint(ctx.FormValue("a"), 10, 64)
		if err != nil {
			return status.INVALID_REQUEST, nil
		}
		password := ctx.FormValue("p")

		username, found, err := getUsername(accountId)
		if err != nil {
			return status.INTERNAL_SERVER_ERROR, nil
		}
		if !found {
			return status.AUTHORIZATION_FAILED, nil
		}

		authorizationStatus := authorize(username, password)
		if authorizationStatus == status.SUCCESS {
			return getAccountInfo(accountId)
		} else {
			return authorizationStatus, nil
		}
	}))
	Get("/payments/getBalance", ReturnDataHandler(func(ctx *fiber.Ctx) (status.Status, interface{}) {
		accountId, err := strconv.ParseUint(ctx.FormValue("a"), 10, 64)
		if err != nil {
			return status.INVALID_REQUEST, nil
		}
		password := ctx.FormValue("p")

		username, found, err := getUsername(accountId)
		if err != nil {
			log.ErrorLogger.Println(err.Error())
			return status.INTERNAL_SERVER_ERROR, nil
		}
		if !found {
			return status.AUTHORIZATION_FAILED, nil
		}

		authorizationStatus := authorize(username, password)
		if authorizationStatus == status.SUCCESS {
			return getBalance(accountId)
		} else {
			return authorizationStatus, nil
		}
	}))
	Get("/payments/getHistory", ReturnDataHandler(func(ctx *fiber.Ctx) (status.Status, interface{}) {
		accountId, err := strconv.ParseUint(ctx.FormValue("a"), 10, 64)
		if err != nil {
			return status.INVALID_REQUEST, nil
		}
		password := ctx.FormValue("p")

		username, found, err := getUsername(accountId)
		if err != nil {
			return status.INTERNAL_SERVER_ERROR, nil
		}
		if !found {
			return status.AUTHORIZATION_FAILED, nil
		}

		authorizationStatus := authorize(username, password)
		if authorizationStatus == status.SUCCESS {
			return getBalance(accountId)
		} else {
			return authorizationStatus, nil
		}
	}))
	Post("/payments/createAccount", ReturnDataHandler(func(ctx *fiber.Ctx) (status.Status, interface{}) {
		data, err := Deserialize(ctx.Body())
		if err != nil {
			return status.INVALID_REQUEST, nil
		}
		username := string(data.GetStringBytes("username"))
		currency := data.GetInt("currency")
		password := string(data.GetStringBytes("password"))

		authorizationStatus := authorize(username, password)
		if authorizationStatus == status.SUCCESS {
			timestamp, timestampStatus := getUniqueTimestamp(username)
			if timestampStatus == status.SUCCESS {
				return createAccount(username, currency, timestamp)
			} else {
				return timestampStatus, nil
			}
		} else {
			return authorizationStatus, nil
		}
	}))
}
