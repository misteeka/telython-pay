package main

import (
	"main/cfg"
	"main/database"
	"main/log"
	"main/server"
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
	server.Init()

	log.InfoLogger.Println("Fiber run")
	panicIfError(server.Run())

	log.InfoLogger.Println("Shutdown...")
	log.InfoLogger.Println("Goodbye!")

}
