package main

import (
	metrictools "../"
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"sort"
	"strings"
)

// MetricIndex GET /metric
func MetricIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST")
	metrics := r.FormValue("metrics")
	starttime := r.FormValue("starttime")
	endtime := r.FormValue("endtime")
	start := gettime(starttime)
	end := gettime(endtime)
	if !checktime(start, end) {
		start = end - 3600*3
	}
	metricList := strings.Split(metrics, ",")
	sort.Strings(metricList)
	var recordList []interface{}
	dataCon := dataService.Get()
	defer dataCon.Close()
	for i, v := range metricList {
		record := make(map[string]interface{})
		if i != 0 && metricList[i-1] == v {
			continue
		}
		metricData, err := redis.Strings(dataCon.Do("ZRANGEBYSCORE", "archive:"+v, start, end))
		if err != nil {
			log.Println(err)
			continue
		}
		record["name"] = v
		record["values"] = metrictools.GenerateTimeseries(metricData)
		recordList = append(recordList, record)
	}
	rst := make(map[string]interface{})
	rst["metrics"] = recordList
	rst["url"] = "/api/v1/metric?metrics=" + metrics
	if body, err := json.Marshal(rst); err == nil {
		w.Write(body)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// MetricCreate POST /metric
func MetricCreate(w http.ResponseWriter, r *http.Request) {
	var items map[string]int
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&items); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST")
	dataCon := dataService.Get()
	defer dataCon.Close()
	for metric, value := range items {
		v, _ := dataCon.Do("GET", metric)
		if v != nil {
			dataCon.Do("HSET", metric, "ttl", value)
		}
	}
}

// MetricShow GET /metric/{:name}
func MetricShow(w http.ResponseWriter, r *http.Request) {
	metric := mux.Vars(r)["name"]
	starttime := r.FormValue("starttime")
	endtime := r.FormValue("endtime")
	start := gettime(starttime)
	end := gettime(endtime)
	if !checktime(start, end) {
		start = end - 3600*3
	}
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PATCH, DELETE")
	recordList := make(map[string]interface{})
	dataCon := dataService.Get()
	defer dataCon.Close()
	metricData, err := redis.Strings(dataCon.Do("ZRANGEBYSCORE", "archive:"+metric, start, end))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	recordList["name"] = metric
	recordList["url"] = "/api/v1/metric/" + metric
	recordList["records"] = metrictools.GenerateTimeseries(metricData)
	if body, err := json.Marshal(recordList); err == nil {
		w.Write(body)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// MetricUpdate PATCH /metric/{:name}
func MetricUpdate(w http.ResponseWriter, r *http.Request) {
	metric := mux.Vars(r)["name"]
	var item metrictools.MetricData
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PATCH, DELETE")
	dataCon := dataService.Get()
	defer dataCon.Close()
	v, _ := dataCon.Do("GET", metric)
	if v != nil {
		dataCon.Do("HMSET", metric, "ttl", item.TTL)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

// MetricDelete DELETE /metric/{:name}
func MetricDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PATCH, DELETE")
	metric := mux.Vars(r)["name"]
	dataCon := dataService.Get()
	defer dataCon.Close()
	_, err := dataCon.Do("DEL", "archive:"+metric)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	_, err = dataCon.Do("DEL", metric)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
