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
	dataservice   *metrictools.RedisService
	configservice *metrictools.RedisService
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
		_, err = this.dataservice.Do("ZADD",
			"archive:"+msg.Key, []interface{}{msg.Timestamp, new_value})
		if err != nil {
			log.Println(err)
			break
		}
		t, _ := redis.Float64(this.configservice.Do("GET", "archivetime:"+msg.Key, nil))
		if time.Now().Unix()-int64(t) > 300 {
			this.writer.Publish(this.archive_topic, []byte(msg.Key))
		}
		body := fmt.Sprintf("%d:%.2f", msg.Timestamp, msg.Value)
		_, err = this.dataservice.Do("SET", "raw:"+msg.Key, body)
		if err != nil {
			log.Println("set raw", err)
			break
		}
		_, err = this.dataservice.Do("SET", msg.Key, new_value)
		if err != nil {
			log.Println("last data", err)
			break
		}
		_, err = this.configservice.Do("SADD", msg.Host, msg.Key)
	}
	return err
}

func (this *MsgDeliver) getRate(msg *metrictools.Record) (float64, error) {
	rst, err := this.dataservice.Do("GET", "raw:"+msg.Key, nil)
	if err != nil {
		return 0, err
	}
	if rst == nil {
		return msg.Value, nil
	}
	var value float64
	t, v, err := metrictools.GetTimestampValue(string(rst.([]byte)))
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
		v, err := this.configservice.Do("KEYS", "trigger:*", nil)
		if err != nil {
			continue
		}
		if v != nil {
			for _, value := range v.([]interface{}) {
				last, err := this.configservice.Do("HGET", string(value.([]byte)), "last")
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
