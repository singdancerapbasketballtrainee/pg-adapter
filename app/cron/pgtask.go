package cron

import (
	"log"
	"pg-adapter/app/dao"
	"strings"
	"time"
)

func exportCron() {
	nowTime := time.Now()
	nowMS := nowTime.Hour()*10000 + nowTime.Minute()*100 + nowTime.Second()

	if _, ok := dao.CronTab[nowMS]; ok {
		taskNames := dao.CronTab[nowMS]
		go tasksExport(taskNames)
	}
}

func tasksExport(taskNames string) {
	tasks := strings.Split(taskNames, ",")
	// 去重去空
	tasks = removeDuplicateElement(tasks)
	for _, taskName := range tasks {
		err := dao.TaskProc(taskName)
		if err != nil {
			log.Println(err.Error())
		}
	}
}
