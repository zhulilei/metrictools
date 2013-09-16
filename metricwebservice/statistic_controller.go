package main

import (
	metrictools "../"
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

func StatisticShow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	name := mux.Vars(r)["name"]
	starttime := r.FormValue("start")
	endtime := r.FormValue("end")
	start := gettime(starttime)
	end := gettime(endtime)
	if !checktime(start, end) {
		start = end - 3600*3
	}
	record_list := make(map[string]interface{})

	data_con := dataservice.Get()
	defer data_con.Close()
	metric_data, err := redis.Strings(data_con.Do("ZRANGEBYSCORE", "archive:"+name, start, end))
	if err != nil {
		log.Println(err)
		return
	}
	record_list["name"] = name
	record_list["values"] = metrictools.GenerateTimeseries(metric_data)

	if body, err := json.Marshal(record_list); err == nil {
		w.Write(body)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
