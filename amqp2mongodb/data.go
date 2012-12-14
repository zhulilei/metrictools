package main

import (
	"github.com/datastream/metrictools"
	"github.com/datastream/metrictools/amqp"
	"github.com/garyburd/redigo/redis"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"regexp"
	"strings"
	"time"
)

func ensure_index(db_session *mgo.Session, dbname string) {
	session := db_session.Copy()
	defer session.Close()
	ticker := time.NewTicker(time.Second * 3600)
	for {
		clist, err := session.DB(dbname).CollectionNames()
		if err != nil {
			time.Sleep(time.Second * 10)
			session.Refresh()
		} else {
			for i := range clist {
				var index mgo.Index
				if rst, _ := regexp.MatchString("(1sec|10sec|1min|5min|10min|15min)", clist[i]); rst {
					index = mgo.Index{
						Key:        []string{"hs", "nm", "ts"},
						Unique:     true,
						DropDups:   true,
						Background: true,
						Sparse:     true,
					}
				}
				if len(index.Key) > 0 {
					if err = session.DB(dbname).C(clist[i]).EnsureIndex(index); err != nil {
						session.Refresh()
						log.Println("make index error: ", err)
					}
				}
			}
			<-ticker.C
		}
	}
}

func insert_record(message_chan chan *amqp.Message, db_session *mgo.Session, dbname string, metric_chan chan string) {
	session := db_session.Copy()
	defer session.Close()
	var err error
	for {
		msg := <-message_chan
		metrics := strings.Split(strings.TrimSpace(msg.Content), "\n")
		for i := range metrics {
			record := metrictools.NewMetric(metrics[i])
			if record != nil {
				if rst, _ := regexp.MatchString("sd[a-z]{1,2}[0-9]{1,2}", record.Nm); rst && record.App == "disk" {
					continue
				}
				if rst, _ := regexp.MatchString("(eth|br|bond)[0-9]{1,2}", record.Nm); !rst && record.App == "interface" {
					continue
				}
				err = session.DB(dbname).C(record.Retention + record.App).Insert(record.Record)
				if err != nil {
					msg.Done <- -1
					go func() { metric_chan <- metrics[i] }()
				} else {
					msg.Done <- 1
				}
			} else {
				log.Println("metrics error:", msg.Content)
			}
		}
	}
}
func redis_notify(pool *redis.Pool, metric_chan chan string) {
	redis_con := pool.Get()
	ticker := time.NewTicker(time.Second)
	go func() {
		redis_con.Flush()
		<-ticker.C
	}()
	for {
		msg := <-metric_chan
		splitname := strings.Split(msg, " ")
		metric := splitname[0]
		value := splitname[1]
		record := metrictools.NewMetric(metric)
		ttl := 90
		if record.Retention == "5min" {
			ttl = 450
		}
		if record.Retention == "10min" {
			ttl = 900
		}
		if record.Retention == "15min" {
			ttl = 1350
		}
		redis_con.Send("SET", metric, value)
		redis_con.Send("EXPIRE", metric, ttl)
		redis_con.Send("PUBLISH", metric, value)
	}
}
func scan_record(db_session *mgo.Session, dbname string, msg_chan chan []byte) {
	session := db_session.Copy()
	defer session.Close()
	var err error
	ticker := time.NewTicker(time.Minute)
	for {
		now := time.Now().Unix()
		var triggers []metrictools.Trigger
		err = session.DB(dbname).C("Triggers").Find(bson.M{"last": bson.M{"$lt": now - 120}}).All(&triggers)
		if err == nil {
			for i := range triggers {
				msg_chan <- []byte(triggers[i].Exp)
			}
		}
		<-ticker.C
	}
}

func trigger_dispatch(deliver_chan chan *amqp.Message, routing_key string, msg_chan chan []byte) {
	for {
		msg_body := <-msg_chan
		msg := &amqp.Message{
			Content: string(msg_body),
			Done:    make(chan int),
			Key:     routing_key,
		}
		deliver_chan <- msg
		go func() {
			<-msg.Done
		}()
	}
}
