package main

import (
	"encoding/json"
	"github.com/datastream/metrictools"
	"github.com/datastream/metrictools/amqp"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"time"
)

func do_notify(db_session *mgo.Session, dbname string, notify_chan chan *amqp.Message) {
	session := db_session.Clone()
	defer session.Close()
	for {
		raw_msg := <-notify_chan
		var notify_msg metrictools.Notify
		var all_notifyaction []metrictools.NotifyAction
		if err := json.Unmarshal([]byte(raw_msg.Content), &notify_msg); err != nil {
			session.DB(dbname).C("NotifyAction").Find(bson.M{"exp": notify_msg.Exp}).All(&all_notifyaction)
			for i := range all_notifyaction {
				now := time.Now().Unix()
				if all_notifyaction[i].Ir || ((now-all_notifyaction[i].Last) > 300 && all_notifyaction[i].Count < 3) {
					var count int
					if (now - all_notifyaction[i].Last) > 300 {
						count = 1
					} else {
						count = all_notifyaction[i].Count + 1
					}
					go send_notify(all_notifyaction[i], notify_msg)
					session.DB(dbname).C("NotifyAction").Update(bson.M{"exp": all_notifyaction[i].Exp, "uri": all_notifyaction[i].Uri}, bson.M{"last": now, "count": count})
					log.Println("send notify:", all_notifyaction[i].Exp, all_notifyaction[i].Uri)
				}
			}
		}
		raw_msg.Done <- 1
	}
}
func send_notify(notifyaction metrictools.NotifyAction, notify metrictools.Notify) {
	scheme, _ := split_uri(notifyaction.Uri)
	switch  scheme {
	case "mailto":
		{
		}
	case "http":
		{
		}
	case "https":
		{
		}
	case "amqp":
		{
		}
	case "xmpp":
		{
		}
	case "sms":
		{
		}
	}
}
func split_uri(uri string) (string, string) {
	var index int
	for index = range uri {
		if uri[index] == ':' {
			break
		}
	}
	return uri[:index], uri[index:]
}
