package main

import (
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
	return nil
}
