package main

import (
	"github.com/datastream/metrictools"
	"io"
	"labix.org/v2/mgo/bson"
	"log"
	"net/http"
	"strings"
)

func metric_controller(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	metricsname := req.FormValue("metricsname") // all
	starttime := req.FormValue("starttime")
	endtime := req.FormValue("endtime")
	start := gettime(starttime)
	end := gettime(endtime)
	if !checktime(start, end) {
		start = end - 3600*3
	}

	nm := strings.Split(metricsname, ",")
	session := db_session.Clone()
	defer session.Close()
	var json string
	for l := range nm {
		m := metrictools.NewLiteMetric(nm[l])
		if m != nil {
			var query []metrictools.Record
			err := session.DB(dbname).C(m.Retention+m.App).Find(bson.M{"hs": m.Hs, "nm": m.Nm, "ts": bson.M{"$gt": start, "$lt": end}}).Sort("ts").All(&query)
			if err != nil {
				log.Printf("query metric error:%s\n", err)
				db_session.Refresh()
			} else {
				json += *json_metrics_value(query, m.App, m.Retention)
			}
		}
	}
	if len(json) > 1 {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "["+json[:len(json)-1]+"]")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, "[]")
	}
}
