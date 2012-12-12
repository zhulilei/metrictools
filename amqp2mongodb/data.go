package main

import (
	"github.com/datastream/metrictools"
	"github.com/datastream/metrictools/amqp"
	"github.com/garyburd/redigo/redis"
	"labix.org/v2/mgo"
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

func insert_record(message_chan chan *amqp.Message, db_session *mgo.Session, dbname string, pool *redis.Pool) {
	session := db_session.Copy()
	defer session.Close()
	redis_con := pool.Get()
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
				splitname := strings.Split(metrics[i], " ")
				metric := splitname[0]
				value := splitname[1]
				redis_con.Send("SET", metric, value)
				redis_con.Send("EXPIRE", metric, 120)
				redis_con.Send("PUBLISH", metric, value)
				redis_con.Flush()
			} else {
				log.Println("metrics error:", msg.Content)
			}
		}
		if err != nil {
			msg.Done <- -1
		} else {
			msg.Done <- 1
		}
	}
}
