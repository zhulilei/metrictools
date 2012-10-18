package main

import (
	"encoding/json"
	"github.com/datastream/cal"
	"github.com/datastream/metrictools"
	"github.com/datastream/metrictools/amqp"
	"github.com/datastream/metrictools/notify"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"regexp"
	"strings"
	"time"
)

func insert_record(message_chan chan *amqp.Message, scan_chan chan *metrictools.Metric, db_session *mgo.Session, dbname string) {
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
				err = session.DB(dbname).C(record.Retention + "_" + record.App).Insert(record.Record)
				splitname := strings.Split(metrics[i], " ")
				host := &metrictools.Host{
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
						time.Sleep(time.Second * 2)
					}
				}
			} else {
				log.Println("metrics error:", msg.Content)
			}
			scan_chan <- record
		}
		if err != nil {
			msg.Done <- -1
		} else {
			msg.Done <- 1
		}
	}
}

func scan_record(message_chan chan *metrictools.Metric, notify_chan chan []byte, db_session *mgo.Session, dbname string) {
	session := db_session.Copy()
	defer session.Close()
	for {
		metric := <-message_chan
		var err error
		var alm []metrictools.Alarm
		metric_full := metric.Retention + metric.App + metric.Nm + metric.Cl + metric.Hs
		err = session.DB(dbname).C("Alarm").Find(bson.M{"exp": bson.M{"$regex": metric_full}}).All(&alm)
		if err != nil {
			log.Println("mongodb error", err)
			time.Sleep(time.Second * 2)
			session.Refresh()
		}
		if len(alm) > 0 {
			go check_metric(alm, notify_chan, session, dbname)
		}
	}
}

func check_metric(alm []metrictools.Alarm, notify_chan chan []byte, db_session *mgo.Session, dbname string) {
	session := db_session.Clone()
	defer session.Close()
	for i := range alm {
		var value float64
		var stat int
		if alm[i].T == metrictools.EXP {
			exps := cal.Parser(alm[i].Exp)
			k_v := make(map[string]float32)
			for i := range exps {
				if len(exps[i]) > 1 {
					result := get_values(exps[i], alm[i].P, session, dbname)
					if len(result) < 1 {
						return
					}
					k_v[exps[i]] = float32(metrictools.Avg_value(result))
				}
			}
			t_value, _ := cal.Cal(alm[i].Exp, k_v)
			value = float64(t_value)
			stat = metrictools.Judge_value(alm[i], value)
		} else {
			result := get_values(alm[i].Exp, alm[i].P, session, dbname)
			if len(result) < 1 {
				return
			}
			stat, value = check_value(alm[i], result)
		}
		go trigger(alm[i].Exp, stat, value, notify_chan, session, dbname)
	}
}

func trigger(metric string, stat int, value float64, notify_chan chan []byte, db_session *mgo.Session, dbname string) {
	session := db_session.Clone()
	defer session.Close()
	var almaction metrictools.AlarmAction
	err := session.DB(dbname).C("AlarmAction").Find(bson.M{"exp": metric}).One(&almaction)
	if err == nil {
		nt := &notify.Notify{
			Alarmaction: almaction,
			Level:       stat,
			Value:       value,
		}
		if almaction.Stat > 0 || almaction.Stat != stat {
			if body, err := json.Marshal(nt); err == nil {
				notify_chan <- body
			}
		}
		if almaction.Stat != stat {
			_ = session.DB(dbname).C("AlarmAction").Update(bson.M{"exp": metric}, bson.M{"stat": stat})
		}
	}
}

func get_values(metric string, interval int, session *mgo.Session, dbname string) []metrictools.Record {
	var result []metrictools.Record
	end := time.Now().Unix()
	start := end - int64(60*interval)
	var hosts []metrictools.Host
	_ = session.DB(dbname).C("host_metric").Find(bson.M{"metric": bson.M{"$regex": metric}}).All(&hosts)

	for i := range hosts {
		m := metrictools.NewMetric(hosts[i].Metric)
		var tmp []metrictools.Record
		err := session.DB(dbname).C(m.Retention + "_" + m.App).Find(bson.M{"hs": m.Hs, "nm": m.Nm, "ts": bson.M{"$gt": start, "$lt": end}}).All(&tmp)
		if err == nil {
			result = append(result, tmp...)
		}
	}
	return result
}

func check_value(v metrictools.Alarm, data []metrictools.Record) (int, float64) {
	var rst float64
	switch v.T {
	case metrictools.AVG:
		{
			rst = metrictools.Avg_value(data)
			return metrictools.Judge_value(v, rst), rst
		}
	case metrictools.SUM:
		{
			rst = metrictools.Sum_value(data)
			return metrictools.Judge_value(v, rst), rst
		}
	case metrictools.MAX:
		{
			rst = metrictools.Max_value(data)
			return metrictools.Judge_value(v, rst), rst
		}
	case metrictools.MIN:
		{
			rst = metrictools.Min_value(data)
			return metrictools.Judge_value(v, rst), rst
		}
	}
	return 0, rst
}
