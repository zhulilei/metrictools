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
	current := time.Now().Unix()
	if stat > 0 {
		last = current - int64(stat)*24*3600
	} else {
		last = current - 300
	}
	if stat == -1 {
		last = current - 60*24*3600
	}
	metric := "archive:" + string(m.Body)
	_, err := this.dataservice.Do("ZREMRANGEBYSCORE",
		metric, []interface{}{0, last})
	if err != nil {
		log.Println("last data", err)
		return err
	}
	this.configservice.Do("SET", "archivetime:"+string(m.Body), time.Now().Unix())
	if stat != 0 {
		go this.do_compress(metric)
	}
	return nil
}

func (this *DataArchive) do_compress(key string) {
	t, err := redis.Float64(this.configservice.Do("GET", "compresstime:"+key, nil))
	if err != nil {
		return
	}
	current := int64(t)
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
			}
			size := len(value_list)
			if size > 0 {
				_, err = this.dataservice.Do("ZADD", key,
					[]interface{}{sumtime / int64(size), sumvalue / float64(size)})
				if err != nil {
					break
				}
			}
			for _, value := range value_list {
				_, err = this.dataservice.Do("ZREM", key, value)
				if err != nil {
					break
				}
			}
			if err != nil {
				break
			}
			current += interval
		} else {
			log.Println("fail to get range", err)
			time.Sleep(time.Second)
		}
	}
	this.configservice.Do("SET", "compresstime:"+key, current)
}
