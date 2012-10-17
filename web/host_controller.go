package main

import (
	"github.com/datastream/metrictools/types"
	"io"
	"labix.org/v2/mgo/bson"
	"log"
	"net/http"
	"strings"
)

func host_controller(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	hosts := req.FormValue("host")
	starttime := req.FormValue("starttime")
	endtime := req.FormValue("endtime")
	start := gettime(starttime)
	end := gettime(endtime)
	if !checktime(start, end) {
		start = end - 360
	}

	host_list := strings.Split(hosts, ",")
	var rsp []types.Host
	session := db_session.Clone()
	defer session.Close()
	for l := range host_list {
		var query []types.Host
		err := session.DB(dbname).C("host_metric").Find(bson.M{"host": host_list[l]}).Sort("metric").All(&query)
		if err != nil {
			log.Printf("query error:%s\n", err)
			db_session.Refresh()
		} else {
			rsp = append(rsp, query...)
		}
	}
	if len(rsp) > 0 {
		w.WriteHeader(http.StatusOK)
		json := json_host_list(rsp)
		io.WriteString(w, *json)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, "internal error")
	}
}
