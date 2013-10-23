package main

import (
	metrictools "../"
	"fmt"
	nsq "github.com/bitly/go-nsq"
	"github.com/garyburd/redigo/redis"
	"log"
	"time"
)

// DataArchive define data archive task
type DataArchive struct {
	dataService *redis.Pool
}

// HandleMessage is DataArchive's nsq handle function
func (m *DataArchive) HandleMessage(msg *nsq.Message) error {
	dataCon := m.dataService.Get()
	defer dataCon.Close()
	stat, _ := redis.Int64(dataCon.Do("HGET", string(msg.Body), "ttl"))
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
	metric := "archive:" + string(msg.Body)
	_, err := dataCon.Do("ZREMRANGEBYSCORE", metric, 0, last)
	if err != nil {
		log.Println("failed to remove old data", metric, err)
		return err
	}
	dataCon.Do("HSET", string(msg.Body), "archivetime", time.Now().Unix())
	m.compress(string(msg.Body), "5mins")
	m.compress(string(msg.Body), "10mins")
	m.compress(string(msg.Body), "15mins")
	return nil
}

func (m *DataArchive) compress(metric string, compresstype string) {
	dataCon := m.dataService.Get()
	defer dataCon.Close()
	t, err := redis.Int64(dataCon.Do("HGET", metric, compresstype))
	if err != nil && err != redis.ErrNil {
		log.Println("failed to get compress time", err)
		return
	}
	var interval int64
	now := time.Now().Unix()
	switch compresstype {
	case "5mins":
		if t == 0 {
			t = now - 3600*24*3
		}
		interval = 300
		if t > (now - 3610*24) {
			return
		}
	case "10mins":
		if t == 0 {
			t = now - 3600*24*7
		}
		interval = 600
		if t > (now - 3600*24*3) {
			return
		}
	case "15mins":
		if t == 0 {
			t = now - 3600*24*15
		}
		interval = 900
		if t > (now - 3600*24*7) {
			return
		}
	}
	metricset := "archive:" + metric
	valueList, err := redis.Strings(dataCon.Do("ZRANGEBYSCORE", metricset, t, t+interval))
	if err == nil {
		sumvalue := float64(0)
		sumtime := int64(0)
		for _, val := range valueList {
			t, v, _ := metrictools.GetTimestampAndValue(val)
			sumvalue += v
			sumtime += t
		}
		size := len(valueList)
		if size > 0 && size != 1 {
			body := fmt.Sprintf("%d:%.2f", sumtime/int64(size), sumvalue/float64(size))
			_, err = dataCon.Do("ZADD", metricset, sumtime/int64(size), body)
			if err != nil {
				return
			}
			_, err = dataCon.Do("ZREMRANGEBYSCORE", metricset, t, t+interval)
			if err != nil {
				log.Println("failed to remove old data", err)
				return
			}
		}
		dataCon.Do("HSET", metric, compresstype, t+interval)
	}
}
