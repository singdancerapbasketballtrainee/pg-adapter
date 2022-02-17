package dao

/*
author:heqimin
purpose:transform data from db rows
*/

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"
)

/**
 * @Description: 用于处理sql请求结果的对象
 */
type colsProc struct {
	// 导出财务文件基本信息
	finName string
	// pg请求结果
	rows     *sql.Rows         // pg result
	colNames []string          // 字段名
	colTypes []*sql.ColumnType //字段类型

	querySql *bytes.Buffer // pg请求结果转化的sql
	// sql处理所需公用项
	sqlHead   string // sql头，replace into
	sqlFmtStr string // 用于格式化values
	// 每行数据
	row *colValue
}

/**
 * @Description: 用于逐行处理sql数据的对象
 */
type colValue struct {
	colNames []string          // 字段名
	colTypes []*sql.ColumnType //字段类型

	// 从pgsql中取出来的数据
	scans  []interface{} //存储values各项地址，用于进行scan操作
	values []sql.RawBytes

	// 组装入库mysql的数据时
	colsScans []interface{} //存储每行的各个字段值，解构[]interface{}进行format
}

/**
 * @Description: pg到mysql数据转换
 * @return error
 */
func (h *exportHandle) startTransform() ([]CodeValue, error) {
	cp, err := h.newColsProc()
	if err != nil {
		return []CodeValue{}, err
	}
	err = cp.transformPrepare()
	if err != nil {
		return []CodeValue{}, err
	}
	// sql请求结果处理初始化
	//cp.querySqlInit()
	return cp.sqlRowsHandle()
}

/**
 * @Description:创建新的sql请求结果处理对象
 * @param rows
 * @return *colsProc
 * @return error
 */
func (h *exportHandle) newColsProc() (*colsProc, error) {
	colNames, err := h.rows.Columns()
	// 部分字段名在pg和mysql中有区别，需要进行转化
	colNames = colsPg2Mysql(colNames)
	if err != nil {
		return nil, err
	}
	colTypes, err := h.rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	bts := new(bytes.Buffer)
	return &colsProc{finName: h.finName, rows: h.rows, colNames: colNames, colTypes: colTypes, querySql: bts}, err
}

/**
 * @Description: 数据转化成mysql入库命令预处理
 * @receiver cp
 * @return err
 */
func (cp *colsProc) transformPrepare() (err error) {
	// 创建处理单行数据的对象并初始化
	cp.row = newValue(cp.colNames)
	cp.row.colTypes = cp.colTypes
	cp.row.valueInit()
	return
}

/**
 * @Description: 将querySql做进一步处理
 * @receiver cp
 * @return error
 */
func (cp *colsProc) querySqlExec(strSql string, cntFlag int) {
	// 进来的Sql以,结尾，需要改成;结尾
	strSql = strings.TrimRight(strSql, ",") + ";"
	var err error
	if err != nil {
		log.Println(err.Error())
	}
}

/**
 * @Description: pg请求结果处理
 * @receiver cp
 * @return []CodeValue
 * @return error
 */
func (cp *colsProc) sqlRowsHandle() ([]CodeValue, error) {
	cv := make([]CodeValue, 0)
	dv := make([]DateValue, 0)
	var lastCode string
	for cp.rows.Next() {
		code, r, err := cp.row.sqlRowHandle(cp.rows)
		if code != lastCode {
			if lastCode != "" {
				cv = append(cv, CodeValue{lastCode, dv})
				dv = make([]DateValue, 0)
			}
			lastCode = code
		}
		if err != nil {
			return []CodeValue{}, err
		}
		dv = append(dv, r)
	}
	cv = append(cv, CodeValue{lastCode, dv})
	return cv, nil
}

/**
 * @Description: pg结果单行处理
 * @receiver c
 * @param rows
 * @return code
 * @return dv
 * @return err
 */
func (c *colValue) sqlRowHandle(rows *sql.Rows) (code string, dv DateValue, err error) {
	dv.Value = make(RowValue)   // map初始化
	err = rows.Scan(c.scans...) // 将scans结构（scans中为values的地址
	if err != nil {
		return
	}
	for i := 0; i < len(c.values); i++ {
		value := string(c.values[i])
		if c.colNames[i] == CODE {
			code = value
			// code不放到date value里
			continue
		}
		if c.colNames[i] == MARKET {
			dv.Market = value
			// market不放到row value里
			continue
		}
		if c.colTypes[i].ScanType() == reflect.TypeOf(time.Time{}) {
			// 时间类型需要特殊处理
			if c.colNames[i] == srcTime {
				// rtime(在mysql中为src-time) 需要保留 YYYY-MM-DD hh:ii:ss.micro 的格式
				t, _ := time.Parse(time.RFC3339Nano, value)
				// src-time不放到row value里面
				dv.SrcTime = t.Format("2006-01-02 15:04:05.000000")
			} else {
				// 将时间的字符串转换成YYYYMMDD形式的整数
				if c.colNames[i] == DATETIME {
					dv.DateTime = date2Int(value)
					// datetime不放到row value里面
					continue
				}
				dv.Value[c.colNames[i]] = strconv.Itoa(date2Int(value))
			}
		} else {
			dv.Value[c.colNames[i]] = value
		}
	}
	return
}

/**
 * @Description: 创建新的单行数据处理对象
 * @param cols
 * @return *colValue
 */
func newValue(cols []string) *colValue {
	length := len(cols)
	scans := make([]interface{}, length)
	values := make([]sql.RawBytes, length)

	// 往mysql入库时不需要market字段，故-1
	colsScans := make([]interface{}, length-1)
	return &colValue{colNames: cols, scans: scans, values: values, colsScans: colsScans}
}

/**
 * @Description: 单行数据处理对象初始化
 * @receiver c
 */
func (c *colValue) valueInit() {
	// 用scans存储values的地址，用于scans结构后给Scan()传入values元素的地址
	for i := range c.values {
		c.scans[i] = &c.values[i]
	}
}

/**
 * @Description: 获取单行数据
 * @receiver c
 * @param fmtStr
 * @param rows
 * @return string
 * @return error
 */
func (c *colValue) getRowValue(fmtStr string, rows *sql.Rows) (string, error) {
	err := rows.Scan(c.scans...) // 将scans结构（scans中为values的地址
	if err != nil {
		return "", err
	}
	for i, j := 0, 0; i < len(c.values); i++ {
		if c.colNames[i] == MARKET {
			// 入库mysql时不需要市场号，continue跳过j++
			continue
		}
		value := string(c.values[i])
		if value == "" {
			c.colsScans[j] = "NULL"
		} else if c.colTypes[i].ScanType() == reflect.TypeOf(time.Time{}) {
			// 时间类型需要特殊处理
			if c.colNames[i] == srcTime {
				// rtime(在mysql中为src-time) 需要保留 YYYY-MM-DD hh:ii:ss.micro 的格式
				t, _ := time.Parse(time.RFC3339Nano, value)
				c.colsScans[j] = t.Format("2006-01-02 15:04:05.000000")
			} else {
				// 将时间的字符串转换成YYYYMMDD形式的整数（mysql中该字段为整数型不加引号
				c.colsScans[j] = strconv.Itoa(date2Int(value))
			}
		} else {
			c.colsScans[j] = value
		}
		j++
	}
	return fmt.Sprintf(fmtStr, c.colsScans...), err
}

/*colsPg2Mysql
 * @Description: 将老版财务数据sql取出来的字段转化为mysql表中的字段,zqdm->code,bbrq->datetime,rtime->src-time
 * @param cols
 * @return []string
 */
func colsPg2Mysql(cols []string) []string {
	for i := range cols {
		//cols[i] = strings.ToLower(cols[i]) //全部转化为小写
		switch cols[i] {
		case ZQDM:
			cols[i] = CODE
		case BBRQ:
			cols[i] = DATETIME
		case RTIME:
			cols[i] = srcTime
		default:
		}
	}
	return cols
}

/*date2Int
 * @Description: 对老版财务数据中所取的日期字段做特殊处理
 * @Description: pgsql中取出来的日期为TZ格式的时间，需要转化为int类型，即写为YYYYMMDD的整数，需要进行相应转换
 * @Description: 特殊处理原因：历史遗留问题，日期和时间均为整数型
 * @param date
 * @return int
 */
func date2Int(timeString string) (dateInt int) {
	t, _ := time.Parse(time.RFC3339Nano, timeString)
	dateYear := t.Year()
	dateMonth := int(t.Month())
	dateDay := t.Day()
	dateInt = dateYear*10000 + dateMonth*100 + dateDay
	return
}

/**
 * @Description: 获取任务下所有财务文件名
 * @param taskName
 * @return []string
 * @return error
 */
func getFins(taskName string) ([]string, error) {
	// 仅限自营，isvalid=2
	querySql := fmt.Sprintf("SELECT finname from %s.%s where taskname = '%s' and isvalid = 2;",
		tables.schemaName, tables.finInfo, taskName)
	rows, err := finDB.Raw(querySql).Rows()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var fins []string
	var fin string
	for rows.Next() {
		err = rows.Scan(&fin)
		if err != nil {
			log.Println(err.Error())
		} else {
			fins = append(fins, fin)
		}
	}
	return fins, err
}

/**
 * @Description: 通过pg库配置表判断财务数据字段是否是时间类型，如果是则返回true
 * @Description: 库配置表中错误太多，该接口暂时废弃
 * @param fieldName
 * @return bool
 */
func isTime(fieldName string) bool {
	fType, ok := pgFieldType[fieldName]
	if ok {
		return fType == pgTypeTime
	}
	// 301 是财务数据的起始字段，且301特殊处理
	// sql本身不区分大小写，查出来的字段名都是小写，但是财务数据配置表中有为大写的，因此使用sql中的lower函数统一转换成小写进行处理
	querySql := fmt.Sprintf("select cj_type from %s.%s where lower(cj_field) = '%s' and (position('已废弃' in cj_table) = 0 or cj_table is null)and dmno > 301;",
		tables.schemaName, tables.fieldInfo, fieldName)
	row := finDB.Raw(querySql).Row()
	err := row.Scan(&fType)
	if err != nil {
		// TODO 错误处理
		return false
	}
	fmt.Println("type of", fieldName, ":", fType)
	pgFieldType[fieldName] = fType
	return fType == pgTypeTime
}
