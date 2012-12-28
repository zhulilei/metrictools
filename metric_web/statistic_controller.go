package main

import (
	"github.com/datastream/metrictools"
	"io"
	"labix.org/v2/mgo/bson"
	"log"
	"net/http"
)

func statistic_controller(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	name := req.FormValue("name") // all
	starttime := req.FormValue("start")
	endtime := req.FormValue("end")
	start := gettime(starttime)
	end := gettime(endtime)
	if !checktime(start, end) {
		start = end - 3600*3
	}

	session := db_session.Clone()
	defer session.Close()
	var json_string string
	var query []metrictools.StatisticRecord
	err := session.DB(dbname).C("StatisticRecord").Find(bson.M{"nm": name, "ts": bson.M{"$gt": start, "$lt": end}}).Sort("ts").All(&query)
	if err != nil {
		log.Printf("query metric error:%s\n", err)
		db_session.Refresh()
	} else {
		json_string += json_statistic_value(query, name)
	}
	if len(json_string) > 1 {
		w.WriteHeader(http.StatusOK)
		if json_string[len(json_string)-1] == ',' {
			json_string = json_string[:len(json_string)-1]
		}
		io.WriteString(w, "["+json_string+"]")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, "[]")
	}
}
