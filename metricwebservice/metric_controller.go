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
	metric_list := strings.Split(metrics, ",")
	sort.Strings(metric_list)
	var record_list []interface{}
	data_con := dataservice.Get()
	defer data_con.Close()
	for i, v := range metric_list {
		record := make(map[string]interface{})
		if i != 0 && metric_list[i-1] == v {
			continue
		}
		metric_data, err := redis.Strings(data_con.Do("ZRANGEBYSCORE", "archive:"+v, start, end))
		if err != nil {
			log.Println(err)
			continue
		}
		record["name"] = v
		record["values"] = metrictools.GenerateTimeseries(metric_data)
		record_list = append(record_list, record)
	}
	rst := make(map[string]interface{})
	rst["metrics"] = record_list
	rst["url"] = "/api/v1/metric?metrics=" + metrics
	if body, err := json.Marshal(rst); err == nil {
		w.Write(body)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

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
	data_con := dataservice.Get()
	defer data_con.Close()
	for metric, value := range items {
		v, _ := data_con.Do("GET", metric)
		if v != nil {
			data_con.Do("HSET", metric, "ttl", value)
		}
	}
}

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
	record_list := make(map[string]interface{})
	data_con := dataservice.Get()
	defer data_con.Close()
	metric_data, err := redis.Strings(data_con.Do("ZRANGEBYSCORE", "archive:"+metric, start, end))
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	record_list["name"] = metric
	record_list["url"] = "/api/v1/metric/" + metric
	record_list["records"] = metrictools.GenerateTimeseries(metric_data)
	if body, err := json.Marshal(record_list); err == nil {
		w.Write(body)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

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
	data_con := dataservice.Get()
	defer data_con.Close()
	v, _ := data_con.Do("GET", metric)
	if v != nil {
		data_con.Do("HMSET", metric, "ttl", item.TTL)
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

func MetricDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PATCH, DELETE")
	metric := mux.Vars(r)["name"]
	data_con := dataservice.Get()
	defer data_con.Close()
	_, err := data_con.Do("DEL", "archive:"+metric)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	_, err = data_con.Do("DEL", metric)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
