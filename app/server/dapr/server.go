package dapr

import (
	"context"
	"fmt"
	"github.com/dapr/go-sdk/service/common"
	"github.com/gin-gonic/gin"
	"net/http"
	"pg-adapter/api"
	"pg-adapter/app/dao"
	negt "pg-adapter/pkg/go-sdk/service/http"
	"time"
)

var svc api.NegtServer

// New Server 服务层，该层封装服务级别的接口函数，
// 如http服务对外提供的url,grpc服务对外提供的proto
// New 提供服务的创建方法，在di中进行依赖注入
func New(s api.NegtServer) (srv common.Service, err error) {
	// 创建路由转发
	r := gin.Default()
	mux := http.NewServeMux()
	// 设置路由句柄
	initRoute(r)
	mux.Handle("/", r)
	// 启动服务
	srv = negt.NewServiceWithMux(fmt.Sprintf(":%d", s.Port()), mux)
	svc = s // 给包变量svc赋值为初始化后的service
	return srv, err
}

//127.0.0.1:9090/hello
// initRoute http请求路由设置
func initRoute(r *gin.Engine) {
	r.GET("/query", queryHandler)
	r.GET("/export", exportHandler) //方便适配老版财务数据业务的后门
	r.GET("/ping", pingHandler)
	r.GET("/cmd", cmdHandler)
}

// cmdHandler 管理命令url
func cmdHandler(c *gin.Context) {
	c.JSON(200, gin.H{})
}

// ping命令
func pingHandler(c *gin.Context) {
	ctx := context.WithValue(context.Background(), "key", "value")
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(svc.Timeout()))
	defer cancel()
	err := svc.Ping(ctx)
	if err != nil {
		c.JSON(500, err)
	} else {
		c.JSON(200, "pong")
	}
}

/**
 * @Description: 查询财务数据
 * @param c
 * @example: 请求示例： curl -X POST localhost:8080/query -d 'datatype=2099&datetime=20210803-20210803&codelist=17()'
 * @example: 参数：
 * @example: datatype: 字段id，以逗号隔开：321,322
 * @example: datetime: 日期，开始日期-结束日期：20210803-20210803
 * @example: codelist: 市场及代码，代码可为空：17(),33(300033)
 */
func queryHandler(c *gin.Context) {
	qp := dao.GetQueryPara(c)
	ctxValue := map[string]interface{}{
		dao.METHOD: dao.QUERY,
		dao.VALUE:  qp,
	}
	ctx := context.WithValue(context.Background(), dao.VALUE, ctxValue)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(svc.Timeout()))
	defer cancel()
	qr := svc.Query(ctx)
	c.JSON(qr.Code, qr)
}

/**
 * @Description: 查询财务数据
 * @Description: 该处接口为为老版财务数据留的后门，通过老版财务数据导出协议请求获取财务文件数据
 * @param c
 * @example: 请求示例：curl -X POST localhost:8080/export -d "schema=shasefin&finname=test_sh.fin,test_sh.fin&type=1&startdate=20210803&enddate=20210803"
 * @example: 参数：
 * @example: schema: 所属市场，对应mysql中库名（沪深不区分level1 level2），例如 shasefin sznsefin stbfin等
 * @example: finname: 财务文件名
 * @example: type: 导出类型，0-5分别是 全量、按日期、按时间、按代码、按实时（本质为选取配置库中对应sql
 * @example: startdate/enddate: 起止日期，例如 20210803
 * @example: codelist: 区别于query中的codelist，此处为纯代码
 */
func exportHandler(c *gin.Context) {
	eq := dao.GetExportPara(c)
	ctxValue := map[string]interface{}{
		dao.METHOD: dao.EXPORT,
		dao.VALUE:  eq,
	}
	ctx := context.WithValue(context.Background(), dao.VALUE, ctxValue)
	ctx, cancel := context.WithDeadline(ctx, time.Now().Add(svc.Timeout()))
	defer cancel()
	qr := svc.Query(ctx)
	c.JSON(qr.Code, qr)
}
