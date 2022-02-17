package dao

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/google/wire"
	"gorm.io/gorm"
)

const (
	METHOD = "method"
	VALUE  = "value"
)

const (
	QUERY = iota
	EXPORT
)

var Provider = wire.NewSet(New, NewDB)

// Dao dao interface
type Dao interface {
	Close()
	Ping(ctx context.Context) error
	Query(ctx context.Context) *QueryRet
}

type dao struct {
	db *gorm.DB
}

/**
 * @Description: 数据请求返回格式
 * @return 示例如下:
	{
		"status_code":0,
		"status_msg":"success",
		"data":[
			{
				"code": "159806",
				"timelist": [
					{
						"datetime": 20210727,
						"src-time": "2021-07-29 08:05:29.945040",
						"market": "19",
						"value": {
							"rqche": "0.00",
							"rqjmce": "0.00",
							"rqmce": "0.00"
						}
					}
				]
			}
		]
	}
*/
type (
	RowValue map[string]interface{} // 每行所有数据

	DateValue struct {
		DateTime int      `json:"datetime"`
		SrcTime  string   `json:"src-time"`
		Market   string   `json:"market"` // 以后可能会有四位市场，所以用string
		Value    RowValue `json:"value"`
	}
	CodeValue struct {
		Code     string      `json:"code"`
		TimeList []DateValue `json:"timelist"`
	}
	SchemaValue struct {
		Schema   string      `json:"schema"`
		Codelist []CodeValue `json:"codelist"`
	}
	HandleRet struct {
		err error
		Sv  SchemaValue
	}
	QueryRet struct {
		Code int           `json:"status_code"`
		Msg  string        `json:"status_msg"`
		Data []SchemaValue `json:"data"`
	}
)

// new a dao and return.
func New(db *gorm.DB) (d Dao, cf func(), err error) {
	return newDao(db)
}

func newDao(db *gorm.DB) (d *dao, cf func(), err error) {
	err = taskMapInit(db)
	d = &dao{
		db,
	}
	cf = d.Close
	return
}

func (d *dao) Close() {

}

func (d *dao) Ping(ctx context.Context) error {
	return nil
}

/*Query
 * @Description: 数据请求处理接口，通过context带入的参数创建数据导出任务
 * @receiver d
 * @param ctx
 * @return error
 */
func (d *dao) Query(ctx context.Context) *QueryRet {
	return StartHandle(ctx.Value(VALUE).(map[string]interface{}))
}

/*GetQueryPara
 * @Description: 解析query请求，获取创建导出任务所需的参数
 * @param c
 * @return
 * @example
 */
func GetQueryPara(c *gin.Context) map[string]string {
	return map[string]string{
		CODELIST: c.PostForm(CODELIST),
		DATATYPE: c.PostForm(DATATYPE),
		DATETIME: c.PostForm(DATETIME),
	}
}

func GetExportPara(c *gin.Context) map[string]string {
	return map[string]string{
		FINNAME:   c.PostForm(FINNAME),
		TYPE:      c.PostForm(TYPE),
		STARTDATE: c.PostForm(STARTDATE),
		ENDDATE:   c.PostForm(ENDDATE),
		CODELIST:  c.PostForm(CODELIST),
	}
}
