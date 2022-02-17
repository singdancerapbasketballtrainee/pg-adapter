package main

import (
	"log"
	"pg-adapter/app/di"
)

func main() {
	app, cleanup, err := di.InitApp()
	if err != nil {
		log.Fatal("err:", err)
	}
	err = app.Start()
	if err != nil {
		log.Fatal("err:", err)
	}
	cleanup()
}
