package main

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
	"strconv"
	"strings"
)

func MetricHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	metrics := r.FormValue("metrics")
	starttime := r.FormValue("starttime")
	endtime := r.FormValue("endtime")
	start := gettime(starttime)
	end := gettime(endtime)
	if !checktime(start, end) {
		start = end - 3600*3
	}

	metric_list := strings.Split(metrics, ",")
	record_list := make(map[string][]interface{})
	for _, v := range metric_list {
		metric_data, err := wb.dataservice.Do("ZRANGEBYSCORE",
			"archive:"+v, []interface{}{start, end})
		if err != nil {
			log.Println(err)
			continue
		}
		md, ok := metric_data.([]interface{})
		if !ok {
			log.Println("not []interface{}")
			return
		}
		var kv []interface{}
		for _, item := range md {
			t_v := strings.Split(string(item.([]byte)), ":")
			if len(t_v) != 2 {
				log.Println("error redis data")
				continue
			}
			t, _ := strconv.ParseInt(t_v[0], 10, 64)
			value, _ := strconv.ParseFloat(t_v[1], 64)
			kv = append(kv, []interface{}{t, value})
		}
		record_list[v] = kv
	}
	w.Write(gen_json(record_list))
}

func MetricCreateHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to read request"))
		log.Println(err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	var metrics []string
	if err = json.Unmarshal(body, &metrics); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("failed to parse json"))
		return
	} else {
		w.WriteHeader(http.StatusOK)
	}
	for _, metric := range metrics {
		wb.configservice.Do("SET", metric, 1)
	}
}

func MetricDeleteHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	w.WriteHeader(http.StatusOK)
	metric := mux.Vars(r)["name"]
	wb.dataservice.Do("DEL", "raw:"+metric, nil)
	wb.dataservice.Do("DEL", "archive:"+metric, nil)
	wb.dataservice.Do("DEL", metric, nil)
	wb.configservice.Do("DEL", metric, nil)
}
