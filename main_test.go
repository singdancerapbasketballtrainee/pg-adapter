package main

import (
	"finance_transform/app/di"
	"log"
	"testing"
)

/**
程序启动测试
*/
func TestMain(m *testing.M) {
	log.Println("====== main test ======")
	_, _, _ = di.InitApp()
}
