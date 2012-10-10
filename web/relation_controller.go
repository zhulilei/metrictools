package main

import (
	"github.com/datastream/metrictools/types"
	"io"
	"labix.org/v2/mgo/bson"
	"log"
	"net/http"
)

func relation_controller(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	w.WriteHeader(http.StatusOK)

	metrics_a := req.FormValue("a")
	metrics_b := req.FormValue("b")
	statistic := req.FormValue("statistic")
	starttime := req.FormValue("starttime")
	endtime := req.FormValue("endtime")
	start := gettime(starttime)
	end := gettime(endtime)
	if !checktime(start, end) {
		start = end - 360
	}

	session := mogo.session.Clone()
	defer session.Close()

	var hosts []types.Host
	err := session.DB(mogo.dbname).C("host_metric").Find(bson.M{"metric": bson.M{"$regex": metrics_a}}).Sort("host_metric").All(&hosts)
	if err != nil || len(hosts) == 0 {
		return
	}

	var json string
	var query []types.Record

	for i := range hosts {
		m := types.NewLiteMetric(hosts[i].Metric)
		var json string
		if m != nil {
			var q []types.Record
			err := session.DB(mogo.dbname).C(m.App).Find(bson.M{"hs": m.Hs, "rt": m.Rt, "nm": m.Nm, "ts": bson.M{"$gt": start, "$lt": end}}).Sort("ts").All(&q)
			if err != nil {
				log.Printf("query error:%s\n", err)
				return
			} else {
				query = append(query, q...)
				json += *json_metrics_value(q, m.App)
			}
		}
	}

	m2 := types.NewLiteMetric(metrics_b)
	var query2 []types.Record
	if m2 != nil {
		err := session.DB(mogo.dbname).C(m2.App).Find(bson.M{"hs": m2.Hs, "rt": m2.Rt, "nm": m2.Nm, "ts": bson.M{"$gt": start, "$lt": end}}).Sort("ts").All(&query2)
		if err != nil {
			log.Printf("query error:%s\n", err)
			return
		} else {
			json += *json_metrics_value(query2, m2.App)
		}
	}
	if len(statistic) > 0 {
		json += compute(query, query2, metrics_a, statistic, len(hosts))
	}

	if len(json) > 0 {
		io.WriteString(w, "["+json[:len(json)-1]+"]")
	} else {
		io.WriteString(w, "internal error")
	}
}

func compute(q1 []types.Record, q2 []types.Record, name, act string, count int) string {
	var rst map[int64]float64
	for i := range q1 {
		rst[q1[i].Ts/60] += q1[i].V
	}
	if act == "avg" {
		for k, _ := range rst {
			rst[k] /= float64(count)
		}
		return gen_value(rst, name)
	}
	if act == "sum" {
		return gen_value(rst, name)
	}
	for k := range q2 {
		if q2[k].V != 0 {
			q2[k].V = 0.0000001
		}
		rst[q2[k].Ts/60] /= q2[k].V
	}
	if len(q2) < 1 {
		return ""
	}
	return gen_value(rst, name+"/"+q2[0].Nm)
}
