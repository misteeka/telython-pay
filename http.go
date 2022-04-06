package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"github.com/gofiber/fiber/v2"
	auth "github.com/misteeka/telython-auth-client"
	"github.com/valyala/fastjson"
	"main/log"
	"strconv"
)

var app *fiber.App

func initFiber() {
	app = fiber.New(fiber.Config{
		DisableStartupMessage: true,
	})
	registerHandlers()
}

func runFiber() error {
	return app.Listen(":8002")
}

func RespondGet(ctx *fiber.Ctx, status Status, resp interface{}) error {
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

func RespondJson(ctx *fiber.Ctx, status Status, json []byte) {

}

func Respond(ctx *fiber.Ctx, status Status) error {
	json := fmt.Sprintf(`{"status": %d}`, status)
	_, err := ctx.WriteString(json)
	return err
}

func Deserialize(jsonBytes []byte) (data *fastjson.Value, err error) {
	var p fastjson.Parser
	data, err = p.ParseBytes(jsonBytes)
	return
}

func authorize(accountId uint64, password string) Status {
	username, found, err := getUsername(accountId)
	if err != nil {
		return INTERNAL_SERVER_ERROR
	}
	if !found {
		return AUTHORIZATION_FAILED
	}
	resp, err := auth.CheckPassword(username, password)
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return INTERNAL_SERVER_ERROR
	}
	if bytes.Equal(resp, auth.AUTHORIZATION_FAILED) {
		return AUTHORIZATION_FAILED
	} else if bytes.Equal(resp, auth.SUCCESS) {
		return SUCCESS
	} else {
		return INTERNAL_SERVER_ERROR
	}
}

func authorizeUsername(username string, password string) Status {
	resp, err := auth.CheckPassword(username, password)
	if err != nil {
		log.ErrorLogger.Println(err.Error())
		return INTERNAL_SERVER_ERROR
	}
	if bytes.Equal(resp, auth.AUTHORIZATION_FAILED) {
		return AUTHORIZATION_FAILED
	} else if bytes.Equal(resp, auth.SUCCESS) {
		return SUCCESS
	} else {
		return INTERNAL_SERVER_ERROR
	}
}

func registerHandlers() {

	app.Post("/payments/sendPayment", func(ctx *fiber.Ctx) error {
		data, err := Deserialize(ctx.Body())
		if err != nil {
			return Respond(ctx, INVALID_REQUEST)
		}
		sender := data.GetUint64("sender")
		receiver := data.GetUint64("receiver")
		amount := data.GetUint64("amount")
		password := string(data.GetStringBytes("password"))

		authorizationStatus := authorize(sender, password)
		if authorizationStatus == SUCCESS {
			return Respond(ctx, sendPayment(sender, receiver, amount))
		} else {
			return Respond(ctx, authorizationStatus)
		}
	})

	app.Get("/payments/getAccountInfo", func(ctx *fiber.Ctx) error {
		accountId, err := strconv.ParseUint(ctx.FormValue("a"), 10, 64)
		if err != nil {
			return Respond(ctx, INVALID_REQUEST)
		}
		password := ctx.FormValue("p")

		authorizationStatus := authorize(accountId, password)
		if authorizationStatus == SUCCESS {
			resp, status := getBalance(accountId)
			return RespondGet(ctx, status, resp)
		} else {
			return Respond(ctx, authorizationStatus)
		}
	})
	app.Get("/payments/getBalance", func(ctx *fiber.Ctx) error {
		accountId, err := strconv.ParseUint(ctx.FormValue("a"), 10, 64)
		if err != nil {
			return Respond(ctx, INVALID_REQUEST)
		}
		password := ctx.FormValue("p")

		authorizationStatus := authorize(accountId, password)
		if authorizationStatus == SUCCESS {
			resp, status := getBalance(accountId)
			return RespondGet(ctx, status, resp)
		} else {
			return Respond(ctx, authorizationStatus)
		}
	})
	app.Get("/payments/getHistory", func(ctx *fiber.Ctx) error {
		accountId, err := strconv.ParseUint(ctx.FormValue("a"), 10, 64)
		if err != nil {
			return Respond(ctx, INVALID_REQUEST)
		}
		password := ctx.FormValue("p")

		authorizationStatus := authorize(accountId, password)
		if authorizationStatus == SUCCESS {
			resp, status := getBalance(accountId)
			return RespondGet(ctx, status, resp)
		} else {
			return Respond(ctx, authorizationStatus)
		}
	})
	app.Post("/payments/createAccount", func(ctx *fiber.Ctx) error {
		data, err := Deserialize(ctx.Body())
		if err != nil {
			return Respond(ctx, INVALID_REQUEST)
		}
		username := string(data.GetStringBytes("username"))
		currency := data.GetInt("currency")
		password := string(data.GetStringBytes("password"))

		authorizationStatus := authorizeUsername(username, password)
		if authorizationStatus == SUCCESS {
			resp, status := createAccount(username, currency)
			return RespondGet(ctx, status, resp)
		} else {
			return Respond(ctx, authorizationStatus)
		}
	})
}
