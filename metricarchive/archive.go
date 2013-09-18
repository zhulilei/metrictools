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
	this.do_compress(string(m.Body), "5mins")
	this.do_compress(string(m.Body), "10mins")
	this.do_compress(string(m.Body), "15mins")
	return nil
}

func (this *DataArchive) do_compress(metric string, compresstype string) {
	data_con := this.dataservice.Get()
	defer data_con.Close()
	t, err := redis.Int64(data_con.Do("HGET", metric, compresstype))
	if err != nil && err != redis.ErrNil {
		log.Println("failed to get compress time", err)
		return
	}
	var interval int64
	switch compresstype {
	case "5mins":
		if t == 0 {
			t = time.Now().Unix() - 3600
		}
		interval = 300
	case "10mins":
		if t == 0 {
			t = time.Now().Unix() - 3600*24*7
		}
		interval = 600
	case "15mins":
		if t == 0 {
			t = time.Now().Unix() - 3600*24*15
		}
		interval = 900
	}
	metricset := "archive:" + metric
	value_list, err := redis.Strings(data_con.Do("ZRANGEBYSCORE", metricset, t, t+interval))
	if err == nil {
		sumvalue := float64(0)
		sumtime := int64(0)
		for _, val := range value_list {
			t, v, _ := metrictools.GetTimestampAndValue(val)
			sumvalue += v
			sumtime += t
		}
		size := len(value_list)
		if size > 0 && size != 1 {
			body := fmt.Sprintf("%d:%.2f", sumtime/int64(size), sumvalue/float64(size))
			_, err = data_con.Do("ZADD", metricset, sumtime/int64(size), body)
			if err != nil {
				return
			}
			_, err = data_con.Do("ZREMRANGEBYSCORE", metricset, t, t+interval)
			if err != nil {
				log.Println("failed to remove old data", err)
				return
			}
		}
		data_con.Do("HSET", metric, compresstype, t+interval)
	}
}
