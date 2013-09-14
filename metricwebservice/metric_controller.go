package main

import (
	metrictools "../"
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"strconv"
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
	record_list := make(map[string][]interface{})
	data_con := dataservice.Get()
	defer data_con.Close()
	for _, v := range metric_list {
		metric_data, err := redis.Strings(data_con.Do("ZRANGEBYSCORE", "archive:"+v, start, end))
		if err != nil {
			log.Println(err)
			continue
		}
		var kv []interface{}
		for _, item := range metric_data {
			t_v := strings.Split(item, ":")
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
	var kv []interface{}
	for _, item := range metric_data {
		t_v := strings.Split(item, ":")
		if len(t_v) != 2 {
			log.Println("error redis data")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		t, _ := strconv.ParseInt(t_v[0], 10, 64)
		value, _ := strconv.ParseFloat(t_v[1], 64)
		kv = append(kv, []interface{}{t, value})
	}
	record_list["key"] = metric
	record_list["values"] = kv
	if body, err := json.Marshal(record_list); err == nil {
		w.Write(body)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func MetricUpdate(w http.ResponseWriter, r *http.Request) {
	metric := mux.Vars(r)["name"]
	var item metrictools.MetricAttribute
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
		data_con.Do("HMSET", metric, "ttl", item.TTL, "state", item.State)
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
