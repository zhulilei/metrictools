package main

import (
	metrictools "../"
	"fmt"
	"github.com/bitly/nsq/nsq"
	"github.com/garyburd/redigo/redis"
	"log"
	"time"
)

type DataArchive struct {
	dataservice   *redis.Pool
	configservice *redis.Pool
}

func (this *DataArchive) HandleMessage(m *nsq.Message) error {
	data_con := this.dataservice.Get()
	defer data_con.Close()
	stat, _ := redis.Int64(data_con.Do("HGET", string(m.Body), "ttl"))
	var last int64
	current := time.Now().Unix()
	if stat > 0 {
		last = current - stat
	} else {
		last = current - 300
	}
	if stat == -1 {
		last = current - 60*24*3600
	}
	metric := "archive:" + string(m.Body)
	_, err := data_con.Do("ZREMRANGEBYSCORE", metric, 0, last)
	if err != nil {
		log.Println("failed to remove old data", metric, err)
		return err
	}
	data_con.Do("HSET", string(m.Body), "archivetime", time.Now().Unix())
	if stat != 0 {
		go this.do_compress(string(m.Body))
	}
	return nil
}

func (this *DataArchive) do_compress(key string) {
	data_con := this.dataservice.Get()
	defer data_con.Close()
	t, err := redis.Float64(data_con.Do("HGET", key, "compresstime"))
	if err != nil && err != redis.ErrNil {
		log.Println("failed to get compress time", err)
		return
	}
	metric := "archive:" + key
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
		value_list, err := redis.Strings(data_con.Do("ZRANGEBYSCORE", metric, current, current+interval))
		if err == nil {
			sumvalue := float64(0)
			sumtime := int64(0)
			for _, val := range value_list {
				t, v, _ := metrictools.GetTimestampAndValue(val)
				sumvalue += v
				sumtime += t
			}
			size := len(value_list)
			if size > 0 {
				body := fmt.Sprintf("%d:%.2f", sumtime/int64(size), sumvalue/float64(size))
				err = data_con.Send("ZADD", metric, sumtime/int64(size), body)
				if err != nil {
					break
				}
			}
			err = data_con.Send("ZREMRANGEBYSCORE", metric, current, current+interval)
			if err != nil {
				log.Println("failed to remove old data", err)
				break
			}
			current += interval
		} else {
			return
		}
	}
	data_con.Send("HSET", key, "compresstime", current)
	if err = data_con.Flush(); err != nil {
		log.Println("failed to compress: ", key, err)
	}
}
