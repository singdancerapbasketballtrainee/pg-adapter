package dao

import "testing"

var d *dao

// TestMain dao层测试主入口
func TestMain(m *testing.M) {
	var err error
	var cf func()
	if d, cf, err = newTestDao(); err != nil {
		panic(err)
	}
	cf()
}
