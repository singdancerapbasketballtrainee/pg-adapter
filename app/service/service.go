package service

import (
	"context"
	"github.com/google/wire"
	"github.com/pkg/errors"
	"pg-adapter/api"
	"pg-adapter/app/config"
	"pg-adapter/app/dao"
	"time"
)

var Provider = wire.NewSet(New, wire.Bind(new(api.NegtServer), new(*Service)))

type Service struct {
	dao     dao.Dao       // 数据层接口
	port    int           // 端口
	timeout time.Duration // 请求超时限制
}

func New(d dao.Dao) (s *Service, cf func(), err error) {
	svcCfg := config.Service()
	s = &Service{
		dao:     d,
		port:    svcCfg.HttpPort,
		timeout: svcCfg.Timeout * time.Second,
	}
	cf = s.Close
	return
}

func (s *Service) Close() {

}

func (s *Service) Ping(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return errors.New("stop")
	default:
		return nil
	}
}

/*Query
 * @Description: 处理query请求，实现对数据请求的超时管理，返回
 * @receiver s
 * @param ctx
 * @return err
 */
func (s *Service) Query(ctx context.Context) (qr *dao.QueryRet) {
	ch := make(chan *dao.QueryRet, 1)
	go func() {
		ch <- s.dao.Query(ctx)
	}()
	select {
	case <-ctx.Done():
		return &dao.QueryRet{Code: 408, Msg: "request time out", Data: make([]dao.SchemaValue, 0)}
	case qr = <-ch:
		if qr.Msg == "" {
			qr.Msg = "succeed"
		}
		return qr
	}
}

func (s *Service) Port() int {
	return s.port
}

func (s *Service) Timeout() time.Duration {
	return s.timeout
}
