package main

import (
	metrictools "../"
	"encoding/json"
	"github.com/bitly/nsq/nsq"
	"github.com/garyburd/redigo/redis"
	"log"
	"strconv"
	"time"
)

type Notify struct {
	*redis.Pool
}

func (this *Notify) HandleMessage(m *nsq.Message) error {
	var notify_msg metrictools.Notify
	var err error
	config_con := this.Get()
	defer config_con.Close()
	if err = json.Unmarshal([]byte(m.Body), &notify_msg); err == nil {
		v, err := config_con.Do("KEYS", "actions:"+notify_msg.Name+"*")
		if err != nil {
			return err
		}
		for _, notifyaction := range v.([]interface{}) {
			uri, _ := redis.String(
				config_con.Do("HGET", string(notifyaction.([]byte)), "uri"))
			rep, _ := redis.Int(
				config_con.Do("HGET", string(notifyaction.([]byte)), "repeat"))
			count, _ := redis.Int(
				config_con.Do("HGET", string(notifyaction.([]byte)), "count"))
			v, _ := config_con.Do("HGET", string(notifyaction.([]byte)), "last")
			var last int64
			if v != nil {
				last, _ = strconv.ParseInt(string(v.([]byte)), 10, 64)
			}
			action := metrictools.NotifyAction{
				Uri:        uri,
				Repeat:     rep,
				Count:      count,
				UpdateTime: last,
			}
			now := time.Now().Unix()
			if action.Repeat > 0 ||
				(now-action.UpdateTime > 300 &&
					action.Count < 3) {
				var count int
				if (now - action.UpdateTime) > 300 {
					count = 1
				} else {
					count = action.Count + 1
				}
				go send_notify(action, notify_msg)
				config_con.Do("HSET", string(notifyaction.([]byte)), "last", count)
			}
		}
	}
	return err
}

//send notify
func send_notify(notifyaction metrictools.NotifyAction, notify metrictools.Notify) {
	scheme, data := split_uri(notifyaction.Uri)
	switch scheme {
	case "mailto":
		{
			log.Println("send mail:",
				data, notify.Name, notify.Level)
		}
	case "http":
		{
			log.Println("send http:",
				data, notify.Name, notify.Level)
		}
	case "https":
		{
			log.Println("send https:",
				data, notify.Name, notify.Level)
		}
	case "mq":
		{
			log.Println("send mq:",
				data, notify.Name, notify.Level)
		}
	case "xmpp":
		{
			log.Println("send xmpp:",
				data, notify.Name, notify.Level)
		}
	case "sms":
		{
			log.Println("send sms:",
				data, notify.Name, notify.Level)
		}
	}
}

// please check uri format before save
func split_uri(uri string) (string, string) {
	var index int
	for index = range uri {
		if uri[index] == ':' {
			break
		}
	}
	return uri[:index], uri[index+1:]
}
