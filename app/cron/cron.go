package cron

import (
	"fmt"
	"github.com/robfig/cron/v3"
)

func init() {
	// 精确到秒
	crontab := cron.New(cron.WithSeconds())
	spec := "*/1 * * * * ?" //cron表达式
	_, err := crontab.AddFunc(spec, exportCron)
	if err != nil {
		fmt.Println(err.Error())
	} else {
		fmt.Println("make cron task successfully!")
	}
	crontab.Start()
}

/*removeDuplicateElement
 * @Description: 去重去空
 * @param stringSlice
 * @return []string
 */
func removeDuplicateElement(stringSlice []string) []string {
	result := make([]string, 0, len(stringSlice))
	temp := map[string]struct{}{}
	for _, item := range stringSlice {
		if item == "" { // 去空
			continue
		}
		if _, ok := temp[item]; !ok { // 去重
			temp[item] = struct{}{}
			result = append(result, item)
		}
	}
	return result
}
