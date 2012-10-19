package main

import (
	"github.com/datastream/metrictools"
	"io"
	"labix.org/v2/mgo/bson"
	"log"
	"net/http"
)

func type_controller(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	w.WriteHeader(http.StatusOK)

	host := req.FormValue("host")
	metric_type := req.FormValue("type")
	starttime := req.FormValue("starttime")
	endtime := req.FormValue("endtime")
	start := gettime(starttime)
	end := gettime(endtime)
	if !checktime(start, end) {
		start = end - 360
	}
	session := db_session.Clone()
	defer session.Close()
	var query []metrictools.Host
	var json string
	if len(metric_type) > 0 {
		err := session.DB(dbname).C("host_metric").Find(bson.M{"host": host, "metric": bson.M{"$regex": metric_type}}).Sort("metric").All(&query)
		if err != nil {
			log.Printf("query error:%s\n", err)
		} else {

			for l := range query {
				m := metrictools.NewLiteMetric(query[l].Metric)
				if m != nil {
					var query []metrictools.Record
					err := session.DB(dbname).C(m.Retention + m.App).Find(bson.M{"hs": m.Hs, "nm": m.Nm, "ts": bson.M{"$gt": start, "$lt": end}}).Sort("ts").All(&query)
					if err != nil {
						log.Printf("query error:%s\n", err)
						db_session.Refresh()
					} else {
						json += json_metrics_value(query, m.App, m.Retention)
					}
				}
			}
		}
	} else {
		err := session.DB(dbname).C("host_metric").Find(bson.M{"host": host}).Sort("metric").All(&query)
		if err != nil {
			log.Printf("query metrictools error:%s\n", err)
			db_session.Refresh()
		} else {
			json = json_host_type(query, host)
		}
	}
	if len(json) > 0 {
		io.WriteString(w, "["+json+"]")
	} else {
		io.WriteString(w, "internal error")
	}
}
