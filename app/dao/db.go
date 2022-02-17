package dao

/*
   author:heqimin
   purpose:pg库信息加载及链接管理
*/

import (
	"fmt"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/gorm/schema"
	"log"
	"pg-adapter/app/config"
	"strconv"
	"strings"
)

/*Handle
 * @Description: 用于对不同协议的导出实现多态
 */
type Handle interface {
	Start()            // 开始导出执行
	Error() error      // 返回错误
	Data() SchemaValue // 返回数据
}

type dbHandle struct {
	dbs         map[string]*gorm.DB // 用于存储所有任务的数据库连接
	tables      dbConfTables        // 用于存储信息表表名
	pgFieldType map[string]string   // 字段名=>类型的形式存储所有字段的类型
}

type (
	// pgConnInfo DNS信息结构体
	pgConnInfo struct {
		host   string
		port   int
		user   string
		passwd string
		dbname string
	}

	// dbConfTables 适配老版财务数据配置表
	dbConfTables struct {
		schemaName string //信息表所属模式
		taskInfo   string //存储任务信息的表名，默认为taskitems
		finInfo    string //存储财务文件信息的表名，默认为tableinfo
		fieldInfo  string //存储财务文件字段信息的表名，默认为CJ_INDEX
		fin2table  string //存储财务文件名与mysql表对应关系的表名
	}

	// procSQLs 五种导出sql
	procSQLs [5]string
)

/**
 * @Description: query类型请求
 */
type (
	finance struct {
		finName   string   // 财务文件名
		dataTypes []string // 所取字段
	}

	queryPara struct {
		marketCodes map[int]string // 代码列表
		startdate   int            // 开始日期
		enddate     int            // 截止日期
	}

	finQuery struct {
		f    finance
		p    queryPara
		err  error
		data SchemaValue
	}
)

/**
 * @Description: export类型请求
 */
type (
	// exportParam 适配老板财务数据导出模式的参数
	exportParam struct {
		procType  int    //导出类型
		startDate int    //开始时间
		endDate   int    //结束时间
		codeList  string //代码列表
	}

	// finExport 导出请求，存储财务文件信息及导出参数
	finExport struct {
		finName string
		p       exportParam
		err     error
		data    SchemaValue
	}
)

//用于存储pg信息的包变量
var (
	finDB       *gorm.DB                      // 用于存储信息表所在库db连接
	tables      dbConfTables                  // 用于存储信息表表名
	taskPgInfos = make(map[string]pgConnInfo) // 用于存储所有任务的数据库连接信息
	dbMap       = make(map[string]*gorm.DB)   // 用dsn=>db形式存储pg的连接，以dsn为key
	pgFieldType = make(map[string]string)     // 字段名=>类型的形式存储所有字段的类型
)

/**
 * @Description: pg链接日志等级，默认为warn
 */
var pgLogLevel = map[string]logger.LogLevel{
	"silent": logger.Silent,
	"error":  logger.Error,
	"warn":   logger.Warn,
	"info":   logger.Info,
}

/*NewDB
 * @Description: 新建DB层接口
 * @Description: 完成pg库的db连接初始化，并返回配置表所在库的连接作为默认db连接
 * @return db
 * @return cf
 * @return err
 */
func NewDB() (db *gorm.DB, cf func(), err error) {
	config.GetConfigure() // 加载配置
	db, err = defaultDbInit()
	finDB = db
	pgInit() // pg初始化
	cf = Close
	return
}

/*Close
 * @Description: 关闭所有db连接
 */
func Close() {
	for _, d := range dbMap {
		db, _ := d.DB()
		_ = db.Close()
	}
}

/*
 * @Description: pg连接初始化
 * @Description: 在main中引入在config之后，配置已加载
 * @return bool
 */
func pgInit() {
	// 初始化任务信息
	errFatal(taskMapInit(finDB))
	// pg连接初始化
	errFatal(setConns())
}

/**
 * @Description: 错误记录并退出（临时，用于初始化、启动失败等
 * @param err
 */
func errFatal(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

/*defaultDbInit
 * @Description: 读取pg配置并连接默认库（即信息所在库
 * @return error
 */
func defaultDbInit() (db *gorm.DB, err error) {
	confTables := config.Tables()
	dbCfg := config.DBCfg()
	db, err = makePgConn(dbCfg.DefaultDSN)
	if err != nil {
		return
	}
	tables.schemaName, tables.taskInfo, tables.finInfo, tables.fieldInfo, tables.fin2table =
		confTables.SchemaName, confTables.TaskInfo, confTables.FinInfo, confTables.FieldInfo, confTables.Fin2Table
	return
}

/*setConns
 * @Description: 将配置库中存储的pg库信息转化为固定格式的dsn并建立[dsn]db的map进行统一pg连接管理
 * @return error
 */
func setConns() (err error) {
	dbCfg := config.DBCfg()
	// 将存储信息表所在库db连接存入
	dbMap[dbCfg.DefaultDSN] = finDB

	for _, info := range taskPgInfos {
		dsn := getPgDSN(info)
		//判读该dsn在map中是否已存在
		if _, ok := dbMap[dsn]; !ok {
			dbMap[dsn], err = getPgConn(info)
		}
	}
	return
}

/**
 * @Description: 提供财务文件任务获取任务pg连接
 * @param taskName
 * @return db
 * @return err
 */
func getTaskDb(taskName string) (db *gorm.DB, err error) {
	info, err := getTaskPgInfo(taskName)
	if err != nil {
		return
	}
	dsn := getPgDSN(info)
	if _, ok := dbMap[dsn]; !ok {
		db, err = getPgConn(info)
		dbMap[dsn] = db
	} else {
		db = dbMap[dsn]
	}
	return
}

/*taskMapInit
 * @Description: 初始化任务信息
 * @return error
 */
func taskMapInit(db *gorm.DB) (err error) {
	querySql := fmt.Sprintf("SELECT taskname,cron,server,username,passwd,database from %s.%s where export = 2;",
		tables.schemaName, tables.taskInfo)
	fmt.Println(querySql)
	rows, err := db.Raw(querySql).Rows() // 使用原生sql进行查询
	if err != nil {
		return
	}
	//逐行处理查询结果
	for rows.Next() {
		var taskName, cron, server, username, passwd, database string
		err = rows.Scan(&taskName, &cron, &server, &username, &passwd, &database)
		if err != nil {
			break
		} else {
			AppendCron(taskName, cron)
			taskPgInfos[taskName] = getInfo(server, username, passwd, database)
		}
	}
	return
}

/*getInfo
 * @Description: 获取数据库连接信息
 * @param server
 * @param username
 * @param passwd
 * @param database
 * @return info
 */
func getInfo(server string, username string, passwd string, database string) (info pgConnInfo) {
	tmp := strings.Split(server, ":")
	info.host = tmp[0]
	info.port, _ = strconv.Atoi(tmp[1])
	info.user = username
	info.passwd = passwd
	info.dbname = database
	return
}

/**
 * @Description: 获取pg库连接
 * @param info
 * @return db
 * @return err
 */
func getPgConn(info pgConnInfo) (db *gorm.DB, err error) {
	return makePgConn(getPgDSN(info))
}

/**
 * @Description: 通过dsn创建pg连接对象
 * @param dsn
 * @return db
 * @return err
 */
func makePgConn(dsn string) (db *gorm.DB, err error) {
	dbCfg := config.DBCfg()
	level := logger.Warn
	if _, ok := pgLogLevel[dbCfg.LogLevel]; ok {
		level = pgLogLevel[dbCfg.LogLevel]
	}
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{
		NamingStrategy: schema.NamingStrategy{
			TablePrefix:   "",
			SingularTable: true,
		},
		Logger: logger.Default.LogMode(level),
	})
	if err != nil {
		return
	}

	// 设置请求超时时间
	db.Exec(fmt.Sprintf("set statement_timeout to %d", dbCfg.QueryTimeout))
	// 连接设置初始化
	d, err := db.DB()
	if err != nil {
		return
	}
	d.SetMaxIdleConns(dbCfg.MaxIdleConns)
	d.SetMaxOpenConns(dbCfg.MaxOpenConns)
	return
}

/**
 * @Description: 将pg库信息转化为相应dsn
 * @param info
 * @return string
 */
func getPgDSN(info pgConnInfo) string {
	pgsqlDSN := fmt.Sprintf("host=%s port=%d user=%s password=%s dbname=%s",
		info.host, info.port, info.user, info.passwd, info.dbname)
	return pgsqlDSN
}
