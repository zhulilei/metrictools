package main

import (
	metrictools "../"
	"labix.org/v2/mgo/bson"
	"log"
	"net/http"
	"strings"
)

func MetricHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	metrics := req.FormValue("metrics") // all
	starttime := req.FormValue("starttime")
	endtime := req.FormValue("endtime")
	start := gettime(starttime)
	end := gettime(endtime)
	if !checktime(start, end) {
		start = end - 3600*3
	}

	metric_list := strings.Split(metrics, ",")
	session := db_session.Clone()
	defer session.Close()
	var record_list []metrictools.Record
	for _, v := range metric_list {
		var query []metrictools.Record
		err := session.DB(dbname).
			C(metric_collection).
			Find(bson.M{"k": v,
			"t": bson.M{"$gt": start, "$lt": end}}).
			Sort("t").All(&query)
		if err != nil {
			log.Printf("query metric error:%s\n", err)
			db_session.Refresh()
		} else {
			record_list = append(record_list, query...)
		}
	}
	w.Write(json_metrics_value(record_list))
}
