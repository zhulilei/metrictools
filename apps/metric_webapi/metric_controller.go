package main

import (
	"../.."
	"encoding/json"
	"fmt"
	"github.com/fzzy/radix/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"sort"
	"strings"
)

// MetricIndex GET /metric
func (q *WebService) MetricIndex(w http.ResponseWriter, r *http.Request) {
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
	client, err := redis.Dial(q.Network, q.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer client.Close()
	user := loginFilter(r, client)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	for i, v := range metricList {
		record := make(map[string]interface{})
		if i != 0 && metricList[i-1] == v {
			continue
		}
		var data []string
		for i := start / 14400; i <= end/14400; i++ {
			value, err := client.Cmd("GET", fmt.Sprintf("archive:%s:%d", user+"_"+v, i)).Str()
			if err != nil {
				log.Println(err)
				continue
			}
			data = append(data, value)
		}
		metricData := metrictools.ParseTimeSeries(data)
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
func (q *WebService) MetricCreate(w http.ResponseWriter, r *http.Request) {
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
	client, err := redis.Dial(q.Network, q.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer client.Close()
	user := loginFilter(r, client)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	for metric, value := range items {
		metric = user + "_" + metric
		reply := client.Cmd("GET", metric)
		if reply.Err != nil {
			client.Cmd("HSET", metric, "ttl", value)
		}
	}
}

// MetricShow GET /metric/{:name}
func (q *WebService) MetricShow(w http.ResponseWriter, r *http.Request) {
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
	client, err := redis.Dial(q.Network, q.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer client.Close()
	user := loginFilter(r, client)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	var data []string
	for i := start / 14400; i <= end/14400; i++ {
		value, err := client.Cmd("GET", fmt.Sprintf("archive:%s:%d", user+"_"+metric, i)).Str()
		if err != nil {
			log.Println(fmt.Sprintf("archive:%s:%d", user+"_"+metric, i), err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		data = append(data, value)
	}
	metricData := metrictools.ParseTimeSeries(data)
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
func (q *WebService) MetricUpdate(w http.ResponseWriter, r *http.Request) {
	metric := mux.Vars(r)["name"]
	item := make(map[string]int)
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PATCH, DELETE")
	client, err := redis.Dial(q.Network, q.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer client.Close()
	user := loginFilter(r, client)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	metric = user + "_" + metric
	reply := client.Cmd("GET", metric)
	if reply.Err != nil {
		client.Cmd("HSET", metric, "ttl", item["ttl"])
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

// MetricDelete DELETE /metric/{:name}
// Todo
func (q *WebService) MetricDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PATCH, DELETE")
	metric := mux.Vars(r)["name"]
	client, err := redis.Dial(q.Network, q.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer client.Close()
	user := loginFilter(r, client)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	metric = user + "_" + metric
	client.Append("DEL", "archive:"+metric)
	client.Append("DEL", metric)
	client.GetReply()
	reply := client.GetReply()
	if reply.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
