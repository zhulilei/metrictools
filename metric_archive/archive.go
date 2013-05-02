package main

import (
	metrictools "../"
	"github.com/bitly/nsq/nsq"
	"github.com/garyburd/redigo/redis"
	"log"
	"time"
)

type DataArchive struct {
	dataservice   *metrictools.RedisService
	configservice *metrictools.RedisService
}

func (this *DataArchive) HandleMessage(m *nsq.Message) error {
	stat, _ := redis.Int(this.configservice.Do("GET", "setting:"+string(m.Body), nil))
	var last int64
	if stat > 0 {
		last = time.Now().Unix() - int64(stat)*24*3600
	} else {
		last = time.Now().Unix() - 300
	}
	if stat == -1 {
		last = time.Now().Unix() - 60*24*3600
	}
	metric := "archive:" + string(m.Body)
	_, err := this.dataservice.Do("ZREMRANGEBYSCORE",
		metric, []interface{}{0, last})
	if err != nil {
		log.Println("last data", err)
		return err
	}
	this.configservice.Do("SET", "archivetime:"+string(m.Body), time.Now().Unix())
	go this.do_compress(metric)
	return nil
}

func (this *DataArchive) do_compress(key string) {
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
		rst, err := this.dataservice.Do("ZRANGEBYSCORE", key,
			[]interface{}{current, current + interval})
		if err == nil {
			value_list := rst.([]interface{})
			sumvalue := float64(0)
			sumtime := int64(0)
			for _, value := range value_list {
				t, v, _ := metrictools.GetTimestampValue(string(value.([]byte)))
				sumvalue += v
				sumtime += t
				this.dataservice.Do("ZREM", key, value)
			}
			size := len(value_list)
			if size > 0 {
				this.dataservice.Do("ZADD", key,
					[]interface{}{sumtime / int64(size), sumvalue / float64(size)})
			}
		}
	}
}
