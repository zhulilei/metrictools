package main

import (
	metrictools "../"
	"encoding/json"
	"github.com/datastream/nsq/nsq"
	"labix.org/v2/mgo/bson"
	"log"
	"time"
)

type Notify struct {
	*metrictools.MsgDeliver
	Collecion string
}

func (this *Notify) HandleMessage(m *nsq.Message) error {
	session := this.MSession.Clone()
	defer session.Close()
	var notify_msg metrictools.Notify
	var all_notifyaction []metrictools.NotifyAction
	var err error
	if err = json.Unmarshal([]byte(m.Body), &notify_msg); err == nil {
		err = session.DB(this.DBName).C(this.Collecion).
			Find(bson.M{"n": notify_msg.Name}).
			All(&all_notifyaction)
		for _, na := range all_notifyaction {
			now := time.Now().Unix()
			if na.Repeat > 0 ||
				(now-na.UpdateTime > 300 &&
					na.Count < 3) {
				var count int
				if (now - na.UpdateTime) > 300 {
					count = 1
				} else {
					count = na.Count + 1
				}
				go send_notify(na, notify_msg)
				session.DB(this.DBName).C(this.Collecion).
					Update(bson.M{
					"n":   na.Name,
					"uri": na.Uri},
					bson.M{"u": now, "c": count})
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
