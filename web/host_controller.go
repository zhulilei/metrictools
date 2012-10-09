package main

import (
	"io"
	"labix.org/v2/mgo/bson"
	"log"
	"net/http"
	"strings"
	"../types"
)

func host_controller(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	w.WriteHeader(http.StatusOK)

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
	session := mogo.session.Clone()
	defer session.Close()
	for l := range host_list {
		var query []types.Host
		err := session.DB(mogo.dbname).C("host_metric").Find(bson.M{"host": host_list[l]}).Sort("metric").All(&query)
		if err != nil {
			log.Printf("query error:%s\n", err)
			break
		} else {
			rsp = append(rsp, query...)
		}
	}
	if len(rsp) > 0 {
		json := json_host_list(rsp)
		io.WriteString(w, *json)
	} else {
		io.WriteString(w, "internal error")
	}
}
