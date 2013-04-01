package main

import (
	metrictools "../"
	"encoding/json"
	"github.com/datastream/nsq/nsq"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"time"
)

func do_notify(db_session *mgo.Session, dbname string, notify_chan chan *metrictools.Message) {
	session := db_session.Clone()
	defer session.Close()
	var err error
	for {
		raw_msg := <-notify_chan
		var notify_msg metrictools.Notify
		var all_notifyaction []metrictools.NotifyAction
		if err = json.Unmarshal([]byte(raw_msg.Body),
			&notify_msg); err == nil {
			session.DB(dbname).C("NotifyAction").
				Find(bson.M{"exp": notify_msg.Exp}).
				All(&all_notifyaction)
			for i := range all_notifyaction {
				now := time.Now().Unix()
				if all_notifyaction[i].Ir ||
					((now-all_notifyaction[i].Last) > 300 &&
						all_notifyaction[i].Count < 3) {
					var count int
					if (now - all_notifyaction[i].Last) > 300 {
						count = 1
					} else {
						count = all_notifyaction[i].Count + 1
					}
					go send_notify(all_notifyaction[i],
						notify_msg)
					session.DB(dbname).
						C("NotifyAction").
						Update(
						bson.M{
							"exp": all_notifyaction[i].Exp,
							"uri": all_notifyaction[i].Uri},
						bson.M{"last": now,
							"count": count})
				}
			}
		}
		raw_msg.ResponseChannel <- &nsq.FinishedMessage{
			raw_msg.Id, 0, true}
	}
}

//send notify
func send_notify(notifyaction metrictools.NotifyAction, notify metrictools.Notify) {
	scheme, data := split_uri(notifyaction.Uri)
	switch scheme {
	case "mailto":
		{
			log.Println("send mail:",
				data, notify.Exp, notify.Level)
		}
	case "http":
		{
			log.Println("send http:",
				data, notify.Exp, notify.Level)
		}
	case "https":
		{
			log.Println("send https:",
				data, notify.Exp, notify.Level)
		}
	case "mq":
		{
			log.Println("send mq:",
				data, notify.Exp, notify.Level)
		}
	case "xmpp":
		{
			log.Println("send xmpp:",
				data, notify.Exp, notify.Level)
		}
	case "sms":
		{
			log.Println("send sms:",
				data, notify.Exp, notify.Level)
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
