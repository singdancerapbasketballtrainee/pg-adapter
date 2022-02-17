package dao

// 该pg包用于适配老版财务文件的逻辑，和其他的数据导入mysql逻辑并未做适配的通用逻辑，后续该部分要逐渐切掉

/*
author:heqimin
purpose:sql加载与执行
*/

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"github.com/pkg/errors"
	"gorm.io/gorm"
	mar "pg-adapter/app/dao/market"
	"strconv"
	"strings"
	"time"
)

// 导出方式，从0-5分别如下
const (
	opAll   = iota //全量导出
	opBbrq         //按日期导出
	opRtime        //按rtime导出
	opReal         //按实时更新导出
	opCode         //按代码导出
)

// 财务数据特殊字段名
const (
	CODE     = "code"
	DATETIME = "datetime"
	srcTime  = "src-time"
	ZQDM     = "zqdm"
	BBRQ     = "bbrq"
	RTIME    = "rtime"
	MARKET   = "market"
)

// 财务数据相关参数字段
const (
	TASKNAME  = "taskname"  // 财务文件所属任务名
	FINNAME   = "finname"   // 财务文件名
	TABLENAME = "tablename" // 表名
	SCHEMA    = "schema"    // 库名
	STARTDATE = "startdate" // 开始日期
	ENDDATE   = "enddate"   // 结束日期
	DATATYPE  = "datatype"  // 字段
	CODELIST  = "codelist"  // 代码
	TYPE      = "type"      // 导出类型
)

// 财务数据类型，对应pg配置库cj_index中的cj_type
const (
	pgTypeChar   = "C" // 字符
	pgTypeDouble = "D" // 浮点
	pgTypeLong   = "L" // 整型
	pgTypeTime   = "T" // 时间
	pgTypeEmpty  = "M" // 空
)

/*exportHandle
 * @Description: 财务文件sql管理&导出&执行
 */
type exportHandle struct {
	finName string // 财务文件名
	// pg库交互
	db      *gorm.DB  //执行所需连接
	rows    *sql.Rows //pg sql 查询结构
	procSql string    //执行的sql
	// 默认非存储过程且不需要操作索引开关
	funcFlag  bool //是否是存储过程，true为是
	indexFlag bool //索引开关，注：财务数据sql性能过差导致finance账号默认索引关闭，部分sql如需使用需要手动开启
}

/*StartHandle
 * @Description: 导出查询处理
 * @param qp
 * @return error
 */
func StartHandle(ctxValue map[string]interface{}) *QueryRet {
	qr := &QueryRet{Data: make([]SchemaValue, 0)}
	handles, err := paraAnalysis(ctxValue)
	if err != nil {
		qr.Code = 400
		// TODO 错误码管理
		// TODO error message管理
		qr.Msg = err.Error()
		return qr
	}
	ch1 := make(chan Handle, 1)
	cnt := 0
	for _, handle := range handles {
		var h = handle
		go func() {
			h.Start()
			ch1 <- h
		}()
	}
	for {
		select {
		case h := <-ch1:
			data := h.Data()
			if len(data.Codelist) != 0 {
				qr.Data = append(qr.Data, h.Data())
			}
			if h.Error() != nil {
				qr.Msg += h.Error().Error()
			}
			cnt++
		}
		if cnt == len(handles) {
			return qr
		}
	}
}

/**
 * @Description: 对query和export两种不同协议的请求的参数进行解析并返回公共接口实现多态
 * @param ctxValue
 * @return handles
 * @return err
 */
func paraAnalysis(ctxValue map[string]interface{}) (handles []Handle, err error) {
	if ctxValue[METHOD].(int) == QUERY {
		handles, err = queryAnalysis(ctxValue[VALUE].(map[string]string))
	} else {
		handles, err = exportAnalysis(ctxValue[VALUE].(map[string]string))
	}
	if err != nil {
		return
	}
	if len(handles) == 0 {
		err = errors.New("no finance datatype matches,please check")
	}
	return
}

/**********************************
 * @ public handle :对query和export 均适用的接口
 **********************************/

/**
 * @Description:sql预处理
 * @Description: TODO sql处理前后示例补充
 * @receiver h
 */
func (h *exportHandle) sqlPreTreat() {
	// 处理存储过程
	if strings.Contains(h.procSql, "{") && strings.Contains(h.procSql, "}") {
		// 财务数据中使用存储过程的sql均为用{}包围且缺select，需要进行处理
		h.procSql = strings.Replace(h.procSql, "{", "select ", 1)
		h.procSql = strings.Replace(h.procSql, "}", ";", 1)
		h.funcFlag = true
	} else {
		// 非存储过程 适配部分sql中有set enable_nestloop=on
		sqlSplit := strings.Split(h.procSql, ";")
		var querySql string
		for _, unitSql := range sqlSplit {
			// 去掉首尾冗余空格
			unitSql = strings.Trim(unitSql, " ")
			if strings.Contains(unitSql, "select") || strings.Contains(unitSql, "SELECT") {
				querySql = unitSql + ";"
				break
			}
			if unitSql != "" {
				// 说明存在需要开启索引开关的操作
				h.indexFlag = true
			}
		}
		h.procSql = querySql
		h.funcFlag = false
	}
}

/**
 * @Description: 财务文件对应sql查询
 * @receiver h
 * @return error
 */
func (h *exportHandle) sqlExec() error {
	if h.funcFlag {
		// 存储过程
		return h.funcExec()
	} else {
		// select 语句
		return h.selectExec()
	}
}

/**
 * @Description: 执行存储过后获取数据
 * @receiver h
 * @return err
 * @return rows
 */
func (h *exportHandle) funcExec() (err error) {
	// 多段连续要BEGIN END
	// 获取存储过程的数据需要先执行生成临时缓存之后再通过fetch进一步获取数据
	h.db.Exec("BEGIN;")
	defer h.db.Exec("END;") //处理完后end
	row := h.db.Raw(h.procSql).Row()
	var strFetchSql string
	if h.db.Error != nil {
		return h.db.Error
	}
	err = row.Scan(&strFetchSql)
	// fetch all in "strFetchSql"
	strFetchSql = fmt.Sprintf("fetch all in %q", strFetchSql)
	h.rows, err = h.db.Raw(strFetchSql).Rows()
	return
}

/**
 * @Description: 执行select语句获取数据
 * @receiver h
 * @return err
 */
func (h *exportHandle) selectExec() (err error) {
	if h.indexFlag {
		h.db.Exec("set enable_nestloop = on;")        //开启索引
		defer h.db.Exec("set enable_nestloop = off;") //执行sql后关闭索引
	}
	h.rows, err = h.db.Raw(h.procSql).Rows()
	return
}

/**
 * @Description: 从文件信息表中获取财务文件所在任务名
 * @param finaName 财务文件名
 * @return taskName 任务名
 */
func getTaskName(finaName string) (taskName string, err error) {
	querySql := fmt.Sprintf("SELECT taskname from %s.%s where finname = '%s' and isvalid = 2;",
		tables.schemaName, tables.finInfo, finaName)
	row := finDB.Raw(querySql).Row()
	err = row.Scan(&taskName)
	if err != nil {
		return
	}
	return
}

/**
 * @Description: 根据财务文件名获取库名
 * @param taskName
 * @return string
 */
func getSchema(finName string) (schemaName string, err error) {
	// 仅限自营 finQuery=2
	querySql := fmt.Sprintf("SELECT distinct schema from %s.%s where finname = '%s' limit 1;",
		tables.schemaName, tables.fin2table, finName)
	row := finDB.Raw(querySql).Row()
	err = row.Scan(&schemaName)
	return
}

/**
 * @Description: 从数据库表中获取相应财务文件的数据库信息
 * @param taskName
 * @return pgInfo
 * @return err
 */
func getTaskPgInfo(taskName string) (pgInfo pgConnInfo, err error) {
	info, ok := taskPgInfos[taskName]
	if ok {
		pgInfo = info
	} else {
		err = fmt.Errorf("task %s not exists", taskName)
	}
	return
}

/**
 * @Description: 获取财务文件对应的所有五种导出sql
 * @param finName
 * @return Sqls
 * @return err
 */
func getProcSqls(finName string) (Sqls procSQLs, err error) {
	querySql := fmt.Sprintf("SELECT allproc,repproc,finproc,realproc,codeproc from %s.%s where finname = '%s' and isvalid = 2",
		tables.schemaName, tables.finInfo, finName)
	row := finDB.Raw(querySql).Row()
	if finDB.Error != nil {
		return
	}
	var allSql, bbrqSql, rtimeSql, realSql, codeSql string
	err = row.Scan(&allSql, &bbrqSql, &rtimeSql, &realSql, &codeSql)
	if err != nil {
		return
	}
	Sqls[opAll], Sqls[opBbrq], Sqls[opRtime], Sqls[opReal], Sqls[opCode] =
		allSql, bbrqSql, rtimeSql, realSql, codeSql
	return
}

/**********************************
 * @ query handle :query 类型请求执行
 **********************************/

/*Start
 * @Description: query类型文件导出
 * @receiver q
 * @return hr
 */
func (q *finQuery) Start() {
	// TODO 对象复用
	h, err := q.NewHandle()
	if err != nil {
		q.err = err
		return
	}
	q.data.Schema, q.err = getSchema(q.f.finName)
	if q.err != nil {
		return
	}
	h.sqlPreTreat()
	if h.funcFlag {
		errMsg := fmt.Sprintf("datatypes %s are in finance %s which doesn`t suit this handle. Please use /export instead.\n",
			q.f.dataTypes, q.f.finName)
		q.err = errors.New(errMsg)
		return
	}
	h.procSql, q.err = q.sqlOperate(h.procSql)
	if q.err != nil {
		return
	}
	q.err = h.sqlExec()
	if q.err != nil {
		return
	}
	defer h.rows.Close()
	q.data.Codelist, q.err = h.startTransform()
	return
}

/*NewHandle
 * @Description: 创建新的财务文件sql管理&执行&导出对象
 * @receiver q
 * @return *exportHandle
 * @return error
 */
func (q *finQuery) NewHandle() (*exportHandle, error) {
	if q.f.finName == "" {
		return nil, errors.New("Lack of finance file name")
	}
	taskName, err := getTaskName(q.f.finName)
	if err != nil {
		return nil, err
	}
	db, err := getTaskDb(taskName)
	if err != nil {
		return nil, err
	}
	sqls, err := getProcSqls(q.f.finName)
	if err != nil {
		return nil, err
	}
	baseSql := sqls[opBbrq]
	baseSql = strings.Replace(baseSql, "[start]", strconv.Itoa(q.p.startdate), 1)
	baseSql = strings.Replace(baseSql, "[end]", strconv.Itoa(q.p.enddate), 1)
	return &exportHandle{finName: q.f.finName, procSql: baseSql, db: db}, nil
}

func (q *finQuery) Error() error {
	return q.err
}

func (q *finQuery) Data() SchemaValue {
	return q.data
}

/**
 * @Description: 清理不在该财务文件下的市场
 * @receiver q
 */
func (q finQuery) marketClean() map[int]string {
	// 创建新map避免出现同地址操作
	marketCodes := make(map[int]string)
	for market, codes := range q.p.marketCodes {
		suffix := mar.GetSchema(market)
		if !strings.Contains(q.f.finName, suffix) {
			continue
		}
		marketCodes[market] = codes
	}
	return marketCodes
}

/**
 * @Description: 对query类型请求进行参数解析
 * @param qp
 * @return handles
 * @return err
 */
func queryAnalysis(qp map[string]string) (handles []Handle, err error) {
	handles = make([]Handle, 0)
	finFields, err := getFinFields(qp[DATATYPE])
	if err != nil {
		return
	}
	datetime := qp[DATETIME]
	codelist := qp[CODELIST]
	fp, err := getFinQueryParams(datetime, codelist)
	if err != nil {
		return
	}
	for fin, fields := range finFields {
		q := &finQuery{f: finance{fin, fields}, p: fp}
		q.p.marketCodes = q.marketClean()
		if len(q.p.marketCodes) != 0 {
			handles = append(handles, q)
		}
	}
	return
}

/**
 * @Description: 对同一个财务文件取多个市场时通过union对多段sql进行连接
 * @Description: TODO sql处理前后示例
 * @receiver q
 * @param originalSql
 * @return sql
 * @return err
 */
func (q finQuery) sqlOperate(originalSql string) (sql string, err error) {
	const BASE = "base"
	// 去掉原sql末尾的空格和;
	originalSql = strings.TrimRight(strings.TrimRight(originalSql, " "), ";")
	baseSql := fmt.Sprintf("with %s as (%s)", BASE, originalSql)
	cols := strings.Join(q.f.dataTypes, ",")
	selectCols := fmt.Sprintf("select %s,%s,%s,%s,%s from %s", ZQDM, MARKET, BBRQ, RTIME, cols, BASE)
	sqls := make([]string, 0)
	for market, codelist := range q.p.marketCodes {
		codes := strings.Split(codelist, ",")
		filter := fmt.Sprintf(" where %s = %d", MARKET, market)
		if codelist != "" {
			// 将codelist组成 and zqdm in ('code01','code01')，例如 and zqdm in ('300033','300093')
			filter = filter + fmt.Sprintf(" and %s in ('%s')", ZQDM, strings.Join(codes, "','"))
		}
		sqls = append(sqls, selectCols+filter)
	}
	return baseSql + strings.Join(sqls, " union ") + ";", nil
}

/**
 * @Description: 通过数据id获取相关的财务文件名，返回涉及到的每个财务文件的相关字段 map[string][]string
 * @param datatype
 * @return finFields
 * @return err
 */
func getFinFields(datatype string) (finFields map[string][]string, err error) {
	if datatype == "" {
		err = errors.New("no datatype please check!")
		return
	}
	finFields = make(map[string][]string)
	querySql := fmt.Sprintf("select cj_field,cj_table from %s.%s where dmno in (%s);",
		tables.schemaName, tables.fieldInfo, datatype)
	rows, err := finDB.Raw(querySql).Rows()
	if err != nil {
		return
	}
	var fieldName, finNames string
	for rows.Next() {
		err = rows.Scan(&fieldName, &finNames)
		if err != nil {
			return
		}
		fins := strings.Split(finNames, ";")
		for _, finName := range fins {
			finFields[finName] = append(finFields[finName], fieldName)
		}
	}
	return
}

/**
 * @Description: 获取财务文件层面的导出参数（区别于请求层面的参数
 * @param datetime startdate-enddate 例如：20210716-20210906，如果为空则为昨天到今天
 * @param codelist
 * @return fp
 * @return err
 */
func getFinQueryParams(datetime string, codelist string) (fp queryPara, err error) {
	dateErr := "error datetime param"
	codeErr := "error codelist param"
	fp.startdate, fp.enddate, err = getDates(datetime)
	if err != nil {
		errMsg := fmt.Sprintf("%s: %s", dateErr, err.Error())
		err = errors.New(errMsg)
		return
	}
	fp.marketCodes, err = getMarketCodes(codelist)
	if err != nil {
		errMsg := fmt.Sprintf("%s: %s", codeErr, err.Error())
		err = errors.New(errMsg)
		return
	}
	return
}

/**
 * @Description: 获取起止日期
 * @param datetime startdate-enddate 例如：20210716-20210906，如果为空则为昨天到今天
 * @return startdate
 * @return enddate
 * @return err
 */
func getDates(datetime string) (startdate int, enddate int, err error) {
	y, m, d := time.Now().Date()
	enddate = y*10000 + int(m)*100 + d
	startdate = enddate - 1
	dates := strings.Split(datetime, "-")
	if len(dates) != 2 {
		err = errors.New(" ")
		return
	}
	if dates[0] != "" {
		startdate, err = strconv.Atoi(dates[0])
	}
	if dates[1] != "" {
		enddate, err = strconv.Atoi(dates[1])
	}
	return
}

/**
 * @Description: 通过codelist获取每个市场下的所有代码
 * @param codelist 示例： 17(),20(),33(300033,)
 * @return mc 示例： map[17:all, 20:all,33:300033]
 * @return err
 */
func getMarketCodes(codelist string) (mc map[int]string, err error) {
	var market int
	mc = make(map[int]string)
	// 确保末尾是),结尾，方便后续处理
	codelist = strings.TrimRight(codelist, ",") + ","
	// 将各个市场间的),替换为; 方便进行拆分
	codelist = strings.ReplaceAll(codelist, "),", ";")
	codes := strings.Split(codelist, ";")
	for _, c := range codes {
		if c == "" {
			continue
		}
		s := strings.Split(c, "(")
		market, err = strconv.Atoi(s[0])
		if err != nil {
			return
		}
		mc[market] = s[1]
	}
	return
}

/**********************************
 * @ export handle :export 以老的协议于/export下的请求，适配老版服务留的后门
 **********************************/

/*Start
 * @Description: 开始执行导出
 * @receiver e
 * @return hr
 */
func (e *finExport) Start() {
	h, err := e.NewHandle()
	if err != nil {
		e.err = err
		return
	}
	e.data.Schema, e.err = getSchema(e.finName)
	if e.err != nil {
		return
	}
	h.sqlPreTreat()
	h.procSql, e.err = e.sqlOperate(h.procSql)
	if e.err != nil {
		return
	}
	e.err = h.sqlExec()
	if e.err != nil {
		return
	}
	defer h.rows.Close()
	e.data.Codelist, e.err = h.startTransform()
	return
}

/*NewHandle
 * @Description: 创建新的财务文件sql管理&执行&导出对象
 * @receiver e
 * @return *exportHandle
 * @return error
 */
func (e *finExport) NewHandle() (*exportHandle, error) {
	if e.finName == "" {
		return nil, errors.New("Lack of finance file name")
	}
	taskName, err := getTaskName(e.finName)
	if err != nil {
		return nil, err
	}
	db, err := getTaskDb(taskName)
	if err != nil {
		return nil, err
	}
	sqls, err := getProcSqls(e.finName)
	if err != nil {
		return nil, err
	}
	baseSql := sqls[opBbrq]
	return &exportHandle{finName: e.finName, procSql: baseSql, db: db}, nil
}

func (e *finExport) sqlOperate(originalSql string) (sql string, err error) {
	if e.p.procType == opRtime || e.p.procType == opBbrq {
		sql = strings.Replace(originalSql, "[start]", strconv.Itoa(e.p.startDate), 1)
		sql = strings.Replace(sql, "[end]", strconv.Itoa(e.p.endDate), 1)
	} else if e.p.procType == opCode {
		codes := strings.Split(e.p.codeList, ",")
		codelist := fmt.Sprintf("'%s'", strings.Join(codes, "','"))
		sql = strings.Replace(sql, "[codelist]", strings.TrimRight(codelist, ","), 1)
	}
	return
}

func (e *finExport) Error() error {
	return e.err
}

func (e *finExport) Data() SchemaValue {
	return e.data
}

/**
 * @Description: 对export类型请求进行参数解析
 * @param qp
 * @return handles
 * @return err
 */
func exportAnalysis(qp map[string]string) (handles []Handle, err error) {
	handles = make([]Handle, 0)
	finNames := qp[FINNAME]
	if finNames == "" {
		err = errors.New("lack of finance name, please transfer parameter \"finname=\"")
		return
	}
	fins := strings.Split(finNames, ",")
	para, err := getExportParams(qp)
	if err != nil {
		return
	}
	for _, fin := range fins {
		handles = append(handles, &finExport{
			finName: fin,
			p:       para,
		})
	}
	return
}

/**
 * @Description: 获取适配老版协议的导出参数
 * @param qp
 * @return p
 * @return err
 */
func getExportParams(qp map[string]string) (p exportParam, err error) {
	if qp[TYPE] == "" {
		p.procType = 2
	} else {
		p.procType, err = strconv.Atoi(qp[TYPE])
	}
	if err != nil {
		return
	}
	y, m, d := time.Now().Date()
	today := y*10000 + int(m)*100 + d
	p.startDate = today - 1
	p.endDate = today
	if qp[STARTDATE] != "" {
		p.startDate, err = strconv.Atoi(qp[STARTDATE])
	}
	if err != nil {
		return
	}
	if qp[ENDDATE] != "" {
		p.endDate, err = strconv.Atoi(qp[ENDDATE])
	}
	if err != nil {
		return
	}
	p.codeList = qp[CODELIST]
	return
}
