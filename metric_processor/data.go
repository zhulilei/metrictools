package main

import (
	"github.com/datastream/metrictools"
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
				if rst, _ := regexp.MatchString("(Trigger|Statistic)", clist[i]); rst {
					index = mgo.Index{
						Key:        []string{"exp"},
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

func insert_record(message_chan chan *metrictools.Message, db_session *mgo.Session, dbname string, metric_chan chan string) {
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
				go func() { metric_chan <- metrics[i] }()
			} else {
				if len(msg.Content) > 1 {
					log.Println("metrics error:", msg.Content)
				}
			}
		}
		if err != nil {
			msg.Done <- -1
		} else {
			msg.Done <- 1
		}
	}
}
func redis_notify(pool *redis.Pool, metric_chan chan string) {
	redis_con := pool.Get()
	for {
		msg := <-metric_chan
		splitname := strings.Split(msg, " ")
		metric := splitname[0]
		value := splitname[1]
		record := metrictools.NewMetric(msg)
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
		_, err := redis_con.Do("SET", metric, value)
		if err != nil {
			redis_con = pool.Get()
			redis_con.Do("SET", metric, value)
		}
		redis_con.Do("EXPIRE", metric, ttl)
		redis_con.Do("SADD", record.Hs, metric)
		redis_con.Do("EXPIRE", record.Hs, ttl)
	}
}
func scan_trigger(db_session *mgo.Session, dbname string, msg_chan chan string) {
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
				msg_chan <- triggers[i].Exp
			}
		}
		<-ticker.C
	}
}

func msg_dispatch(deliver_chan chan *metrictools.Message, routing_key string, msg_chan chan string) {
	for {
		msg_body := <-msg_chan
		msg := &metrictools.Message{
			Content: msg_body,
			Done:    make(chan int),
			Key:     routing_key,
		}
		deliver_chan <- msg
		go func() {
			<-msg.Done
		}()
	}
}
