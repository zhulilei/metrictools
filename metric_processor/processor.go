package main

import (
	metrictools "../"
	"encoding/json"
	"fmt"
	"github.com/bitly/nsq/nsq"
	"github.com/garyburd/redigo/redis"
	"log"
	"strconv"
	"time"
)

type MsgDeliver struct {
	dataservice   *redis.Pool
	configservice *redis.Pool
	writer        *nsq.Writer
	trigger_topic string
	archive_topic string
	nsqd_addr     string
}

func (this *MsgDeliver) HandleMessage(m *nsq.Message) error {
	var err error
	var c []metrictools.CollectdJSON
	if err = json.Unmarshal(m.Body, &c); err != nil {
		log.Println(err)
		return nil
	}
	for _, v := range c {
		if len(v.Values) != len(v.DSNames) {
			continue
		}
		if len(v.Values) != len(v.DSTypes) {
			continue
		}
		msgs := v.ToRecord()
		if err := this.PersistData(msgs); err != nil {
			return err
		}
	}
	return nil
}

func (this *MsgDeliver) PersistData(msgs []*metrictools.Record) error {
	var err error
	for _, msg := range msgs {
		var new_value float64
		if msg.DSType == "counter" || msg.DSType == "derive" {
			new_value, err = this.getRate(msg)
		} else {
			new_value = msg.Value
		}
		if err != nil && err.Error() == "ignore" {
			continue
		}
		if err != nil {
			log.Println("fail to get new value", err)
			return err
		}
		redis_con := this.dataservice.Get()
		defer redis_con.Close()
		_, err := redis_con.Do("ZADD", "archive:"+msg.Key,
			msg.Timestamp, new_value)
		if err != nil {
			log.Println(err)
			break
		}
		redis_con2 := this.configservice.Get()
		defer redis_con2.Close()
		t, _ := redis.Float64(redis_con2.Do("GET", "archivetime:"+msg.Key))
		if time.Now().Unix()-int64(t) > 600 {
			this.writer.Publish(this.archive_topic, []byte(msg.Key))
		}
		body := fmt.Sprintf("%d:%.2f", msg.Timestamp, msg.Value)
		_, err = redis_con.Do("SET", "raw:"+msg.Key, body)
		if err != nil {
			log.Println("set raw", err)
			break
		}
		_, err = redis_con.Do("SET", msg.Key, new_value)
		if err != nil {
			log.Println("last data", err)
			break
		}
		_, err = redis_con2.Do("SADD", msg.Host, msg.Key)
	}
	return err
}

func (this *MsgDeliver) getRate(msg *metrictools.Record) (float64, error) {
	redis_con := this.dataservice.Get()
	defer redis_con.Close()
	rst, err := redis_con.Do("GET", "raw:"+msg.Key)
	if err != nil {
		return 0, err
	}
	if rst == nil {
		return msg.Value, nil
	}
	var value float64
	t, v, err := metrictools.GetTimestampAndValue(string(rst.([]byte)))
	if err == nil {
		value = (msg.Value - v) / float64(msg.Timestamp-t)
	} else {
		value = msg.Value
	}
	if value < 0 {
		value = 0
	}
	return value, nil
}

func (this *MsgDeliver) ScanTrigger() {
	ticker := time.Tick(time.Minute)
	for {
		now := time.Now().Unix()
		redis_con := this.configservice.Get()
		defer redis_con.Close()
		v, err := redis_con.Do("KEYS", "trigger:*")
		if err != nil {
			continue
		}
		if v != nil {
			for _, value := range v.([]interface{}) {
				last, err := redis_con.Do("HGET", string(value.([]byte)), "last")
				if err != nil {
					continue
				}
				if last != nil {
					d, _ := strconv.ParseInt(string(last.([]byte)), 10, 64)
					if now-d < 61 {
						continue
					}
				}
				_, _, err = this.writer.Publish(this.trigger_topic, value.([]byte))
				if err != nil {
					this.writer.ConnectToNSQ(this.nsqd_addr)
				}
			}
		}
		<-ticker
	}
}
