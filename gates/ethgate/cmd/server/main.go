package main

import (
	"main/pkg/cfg"
	"main/pkg/database"
	"main/pkg/ethapi"
	"main/pkg/http"
	"main/pkg/log"
	"math/rand"
	"time"
)

func main() {
	log.InfoLogger.Println("Starting...")
	var err error
	rand.Seed(time.Now().UnixNano())

	log.InfoLogger.Println("Loading config...")
	err = cfg.LoadConfig()
	if err != nil {
		goto Shutdown
	}

	log.InfoLogger.Println("Database start")
	err = database.InitDatabase()
	if err != nil {
		goto Shutdown
	}

	log.InfoLogger.Println("Ethapi start")
	err = ethapi.Init()
	if err != nil {
		goto Shutdown
	}

	log.InfoLogger.Println("Fiber initializing")
	http.Init()
	registerHandlers()

	log.InfoLogger.Println("Fiber run")
	err = http.Run()
	if err != nil {
		goto Shutdown
	}

Shutdown:
	log.InfoLogger.Println("Shutdown...")
	shutdown()
	log.InfoLogger.Println("Goodbye!")
}
