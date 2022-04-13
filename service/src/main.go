package main

import (
	"main/pkg/cfg"
	"main/pkg/database"
	"main/pkg/http"
	"main/pkg/log"
	"math/rand"
	"runtime"
	"time"
)

func panicIfError(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	runtime.GOMAXPROCS(16)
	rand.Seed(time.Now().UnixNano())

	log.InfoLogger.Println("Starting...")
	log.InfoLogger.Println("Config loading")
	panicIfError(cfg.LoadConfig())
	log.InfoLogger.Println("Database start")
	panicIfError(database.InitDatabase())

	log.InfoLogger.Println("Fiber initializing")
	http.Init()
	registerHandlers()

	log.InfoLogger.Println("Fiber run")
	panicIfError(http.Run())

	log.InfoLogger.Println("Shutdown...")
	log.InfoLogger.Println("Goodbye!")

}
