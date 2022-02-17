package config

import (
	"flag"
	"github.com/go-yaml/yaml"
	"io/ioutil"
	"log"
	"time"
)

type (
	PgConfig struct {
		DefaultDSN   string        `yaml:"DefaultDSN"`   // postgres default database connect dsn
		QueryTimeout time.Duration `yaml:"QueryTimeout"` // time out of pg query (dimension:millisecond
		MaxIdleConns int           `yaml:"MaxIdleConns"` // max number of idles existed
		MaxOpenConns int           `yaml:"MaxOpenConns"` // max number of idles opened
		LogLevel     string        `yaml:"LogLevel"`     // log level of pg connection
	}
	CfgTableConfig struct {
		SchemaName string `yaml:"SchemaName"` // name of the schema in which all finance message tables are
		TaskInfo   string `yaml:"TaskInfo"`   // name of the table which stores every export task`s message of finance
		FinInfo    string `yaml:"FinInfo"`    // name of the table which stores every finance file`s message
		FieldInfo  string `yaml:"FieldInfo"`  // name of the table which stores every finance field`s message
		Fin2Table  string `yaml:"Fin2Table"`  // name of the table which stores corresponding tables for all finance files

	}
	ServiceConfig struct {
		HttpPort        int           `yaml:"HttpPort"`        // http port
		Timeout         time.Duration `yaml:"Timeout"`         // http query time out(dimension:second
		SubscribeServer []string      `yaml:"SubscribeServer"` // servers which subscribe this server     host:port
	}

	SettingConfig struct {
		RowLimit    int    `yaml:"RowLimit"`    // limit of row numbers in a process
		LogPath     string `yaml:"LogPath"`     // log file path
		StatLogPath string `yaml:"StatLogPath"` // status log file path
	}

	Config struct {
		DbCfg    PgConfig       `yaml:"DbCfg"`    // pgsql database connection configure
		CfgTable CfgTableConfig `yaml:"CfgTable"` // finance configure tables configure
		Service  ServiceConfig  `yaml:"Service"`  // service configure
		Setting  SettingConfig  `yaml:"Setting"`  // path & other base setting configure
	}
)

var (
	cfgFile   *string
	configure Config
)

/*GetConfigure
 * @Description: 获取配置
 */
func GetConfigure() {
	// 注释中为线上环境配置
	//cfgFile = flag.String("f", "/usr/local/conf/conf.yaml", "config file path")
	cfgFile = flag.String("f", "/Users/heqimin/Code/Go/finance/pg-adapter/conf/conf.yaml", "config file path")
	flag.Parse()
	content, err := ioutil.ReadFile(*cfgFile)
	if yaml.Unmarshal(content, &configure) != nil {
		log.Fatalf("解析config.yaml出错: %v", err)
	}
}

func DBCfg() PgConfig {
	return configure.DbCfg
}

func Tables() CfgTableConfig {
	return configure.CfgTable
}

func Service() ServiceConfig {
	return configure.Service
}

func Setting() SettingConfig {
	return configure.Setting
}
