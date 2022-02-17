package api

import (
	"context"
	"pg-adapter/app/dao"
	"time"
)

// NegtServer 对外接口
type NegtServer interface {
	Ping(ctx context.Context) error
	Query(ctx context.Context) *dao.QueryRet
	Port() int
	Timeout() time.Duration
}
