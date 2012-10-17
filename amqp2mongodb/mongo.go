package main

import (
	"github.com/datastream/metrictools/types"
	"labix.org/v2/mgo"
	"log"
	"regexp"
	"strings"
	"time"
)

func insert_record(message_chan chan *types.Message, session *mgo.Session, dbname string) {
	defer session.Close()
	for {
		var err error
		msg := <-message_chan
		metrics := strings.Split(strings.TrimSpace(msg.Content), "\n")
		for i := range metrics {
			record := types.NewMetric(metrics[i])
			if record != nil {
				if rst, _ := regexp.MatchString("sd[a-z]{1,2}[0-9]{1,2}", record.Nm); rst && record.App == "disk" {
					continue
				}
				if rst, _ := regexp.MatchString("(eth|br|bond)[0-9]{1,2}", record.Nm); !rst && record.App == "interface" {
					continue
				}
				err = session.DB(dbname).C(record.App).Insert(record.Record)
				splitname := strings.Split(metrics[i], " ")
				host := &types.Host{
					Host:   record.Hs,
					Metric: splitname[0],
					Ttl:    -1,
				}
				err = session.DB(dbname).C("host_metric").Insert(host)
				if err != nil {
					if rst, _ := regexp.MatchString("dup", err.Error()); rst {
						err = nil
					} else {
						log.Println("mongodb insert failed", err)
						session.Refresh()
						time.Sleep(time.Second*2)
					}
				}
			} else {
				log.Println("metrics error:", msg.Content)
			}
		}
		if err != nil {
			go func() {
				message_chan <- msg
			}()
		}
		msg.Done <- 1
	}
}
