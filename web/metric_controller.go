package main

import (
	"github.com/datastream/metrictools/types"
	"io"
	"labix.org/v2/mgo/bson"
	"log"
	"net/http"
	"strings"
)

func metric_controller(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	w.WriteHeader(http.StatusOK)
	metricsname := req.FormValue("metricsname") // all
	starttime := req.FormValue("starttime")
	endtime := req.FormValue("endtime")
	start := gettime(starttime)
	end := gettime(endtime)
	if !checktime(start, end) {
		start = end - 3600*3
	}

	nm := strings.Split(metricsname, ",")
	session := mogo.session.Clone()
	defer session.Close()
	var json string
	for l := range nm {
		m := types.NewLiteMetric(nm[l])
		if m != nil {
			var query []types.Record
			err := session.DB(mogo.dbname).C(m.App).Find(bson.M{"hs": m.Hs, "rt": m.Rt, "nm": m.Nm, "ts": bson.M{"$gt": start, "$lt": end}}).Sort("ts").All(&query)
			if err != nil {
				log.Printf("query error:%s\n", err)
			} else {
				json += *json_metrics_value(query, m.App)
			}
		}
	}
	if len(json) > 1 {
		io.WriteString(w, "["+json[:len(json)-1]+"]")
	} else {
		io.WriteString(w, "[]")
	}
}
