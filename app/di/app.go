package di

import (
	"github.com/dapr/go-sdk/service/common"
	"pg-adapter/app/service"
)

//go:generate wire
type App struct {
	svc     *service.Service
	httpSvc common.Service
}

func NewApp(svc *service.Service, h common.Service) (app *App, closeFunc func(), err error) {
	app = &App{
		svc:     svc,
		httpSvc: h,
	}
	closeFunc = func() {
		err = h.Stop()
	}
	return
}

func (a *App) Start() error {
	return a.httpSvc.Start()
}
