package dao

import (
	"log"
	"strconv"
	"strings"
	"time"
)

type (
	CronTask map[int]string
)

var (
	CronTab = make(CronTask) // 用于存储所有任务的定时任务时间
)

func TaskProc(taskName string) error {
	log.Println("start to handle task:", taskName)
	finNames, err := getFins(taskName)
	if err != nil {
		// TODO log
		return err
	}
	year, month, day := time.Now().Date()
	//ret := &QueryRet{Data: make([]SchemaValue, 0)}
	// go里重定义了month类型
	para := exportParam{
		opRtime,
		year*10000 + int(month)*100 + day,
		year*10000 + int(month)*100 + day,
		"",
	}
	for _, finName := range finNames {
		e := &finExport{
			finName: finName,
			p:       para,
		}
		e.Start()
		if err != nil {
			log.Println(err.Error())
		}
	}
	return nil
}

/*AppendCron
 * @Description: 将数据库获得的map
 * @param crons
 */
func AppendCron(taskName string, cron string) {
	crons := GetCron(cron)
	for _, v := range crons {
		CronTab[v] += taskName + ","
	}
}

/*GetCron
 * @Description: 将数据库中记录的日期字符串转化为整数切片
 * @param crons: 财务文件定时任务格式一般为yy:mm;yy:mm;yy:mm
 * @return cronTab
 */
func GetCron(crons string) []int {
	var cronTab []int
	times := strings.Split(crons, ";")
	for _, v := range times {
		if v == "" {
			continue
		}
		ti := getTime(v)
		cronTab = append(cronTab, ti)
	}
	return cronTab
}

/*getTime
 * @Description: 将时分转化成整数形式
 * @param ti yy:mm格式的字符串（也支持yy:mm:ss
 * @return T 时间
 */
func getTime(ti string) (T int) {
	hms := strings.Split(ti, ":")
	hour, _ := strconv.Atoi(hms[0])
	minute, second := 0, 0
	//防止瞎写
	if len(hms) == 3 { //时分或时分秒
		minute, _ = strconv.Atoi(hms[1])
		second, _ = strconv.Atoi(hms[2])
	} else if len(hms) == 2 {
		minute, _ = strconv.Atoi(hms[1])
	}
	// 防止有人瞎写
	hour %= 24
	minute %= 60
	second %= 60
	T = hour*10000 + minute*100 + second
	return
}
