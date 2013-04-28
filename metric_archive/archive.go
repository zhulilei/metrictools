package main

import (
	metrictools "../"
	"log"
	"time"
)

type DataArchive struct {
	*metrictools.RedisService
}

func (this *DataArchive) CompressData() {
	tick := time.Tick(time.Minute * 10)
	for {
		rst, err := this.Do("KEYS", "archive:*", nil)
		if err == nil {
			value_list := rst.([]interface{})
			for _, value := range value_list {
				this.remove_old(value.([]byte))
				this.do_compress(value.([]byte))
			}
		}
		<-tick
	}
}

func (this *DataArchive) remove_old(key []byte) {
	lastweek := time.Now().Unix() - 60*24*3600
	_, err := this.Do("ZREMRANGEBYSCORE",
		string(key), []interface{}{0, lastweek})
	if err != nil {
		log.Println("last data", err)
	}
}

func (this *DataArchive) do_compress(key []byte) {
	current := time.Now().Unix() - 60*24*3600
	last_d := time.Now().Unix() - 24*3600
	last_2d := time.Now().Unix() - 2*24*3600
	var interval int64
	for {
		if current > last_d {
			break
		}
		if current < last_2d {
			interval = 600
		} else {
			interval = 300
		}
		rst, err := this.Do("ZRANGEBYSCORE", string(key),
			[]interface{}{current, current + interval})
		if err == nil {
			value_list := rst.([]interface{})
			sumvalue := float64(0)
			sumtime := int64(0)
			for _, value := range value_list {
				t, v, _ := this.GetTimestampValue(string(value.([]byte)))
				sumvalue += v
				sumtime += t
				this.Do("ZREM", string(key), value)
			}
			size := len(value_list)
			if size > 0 {
				this.Do("ZADD", string(key),
					[]interface{}{sumtime / int64(size), sumvalue / float64(size)})
			}
		}
	}
}
