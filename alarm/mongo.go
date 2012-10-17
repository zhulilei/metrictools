package main

import (
	"github.com/datastream/cal"
        "github.com/datastream/metrictools/types"
	"github.com/datastream/metrictools/notify"
        "labix.org/v2/mgo"
        "labix.org/v2/mgo/bson"
        "log"
        "strings"
        "time"
)

func scan_record(message_chan chan *types.Message, notify_chan chan *notify.Notify, session *mgo.Session, dbname string) {
	defer session.Close()
	for {
		msg := <-message_chan
		metrics := strings.Split(strings.TrimSpace(msg.Content), "\n")
		var err error
		for i := range metrics {
			metric := strings.Split(metrics[i], " ")
			if len(metric) < 1 {
				continue
			}
			if len(strings.TrimSpace(metric[0])) < 1 {
				continue
			}
			var alm []types.Alarm
			err = session.DB(dbname).C("Alarm").Find(bson.M{"exp": bson.M{"$regex": metric[0]}}).All(&alm)
			if err != nil {
				log.Println("mongodb error", err)
				time.Sleep(time.Second*2)
				session.Refresh()
			}
			if len(alm) > 0 {
				go check_metric(alm, notify_chan, session, dbname)
			}
		}
		if err != nil {
			msg.Done <- -1
		} else {
			msg.Done <- 1
		}
	}
}

func check_metric(alm []types.Alarm, notify_chan chan *notify.Notify, db_session *mgo.Session, dbname string) {
	session := db_session.Clone()
	defer session.Close()
	for i := range alm {
		var value float64
		var stat int
		if alm[i].T == types.EXP {
			exps := cal.Parser(alm[i].Exp)
			k_v := make(map[string]float32)
			for i := range exps {
				if len(exps[i]) > 1 {
					result := get_values(exps[i], alm[i].P, session, dbname)
					if len(result) < 1 {
						return
					}
					k_v[exps[i]] = float32(types.Avg_value(result))
				}
			}
			t_value, _ := cal.Cal(alm[i].Exp, k_v)
			value = float64(t_value)
			stat = types.Judge_value(alm[i], value)
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

func check_value(v types.Alarm, data []types.Record) (int, float64) {
	var rst float64
	switch v.T {
	case types.AVG:
		{
			rst = types.Avg_value(data)
			return types.Judge_value(v, rst), rst
		}
	case types.SUM:
		{
			rst = types.Sum_value(data)
			return types.Judge_value(v, rst), rst
		}
	case types.MAX:
		{
			rst = types.Max_value(data)
			return types.Judge_value(v, rst), rst
		}
	case types.MIN:
		{
			rst = types.Min_value(data)
			return types.Judge_value(v, rst), rst
		}
	}
	return 0, rst
}

func trigger(metric string, stat int, value float64, notify_chan chan *notify.Notify, db_session *mgo.Session, dbname string) {
	var almaction types.AlarmAction
	session := db_session.Clone()
	defer session.Close()
	err := session.DB(dbname).C("AlarmAction").Find(bson.M{"exp": metric}).One(&almaction)
	if err == nil {
		nt := &notify.Notify{
			Alarmaction: almaction,
			Level:       stat,
			Value:       value,
		}
		if almaction.Stat > 0 {
			notify_chan <- nt
		}
		if almaction.Stat != stat {
			_ = session.DB(dbname).C("AlarmAction").Update(bson.M{"exp": metric}, bson.M{"stat": stat})
		}
	}
}

func get_values(metric string, interval int, session *mgo.Session, dbname string) []types.Record {
	var result []types.Record
	end := time.Now().Unix()
	start := end - int64(60*interval)
	var hosts []types.Host
	_ = session.DB(dbname).C("host_metric").Find(bson.M{"metric": bson.M{"$regex": metric}}).All(&hosts)

	for i := range hosts {
		m := types.NewMetric(hosts[i].Metric)
		var tmp []types.Record
		err := session.DB(dbname).C(m.App).Find(bson.M{"hs": m.Hs, "rt": m.Rt, "nm": m.Nm, "ts": bson.M{"$gt": start, "$lt": end}}).All(&tmp)
		if err == nil {
			result = append(result, tmp...)
		}
	}
	return result
}
