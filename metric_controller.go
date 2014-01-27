package main

import (
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
	for i, v := range metricList {
		record := make(map[string]interface{})
		if i != 0 && metricList[i-1] == v {
			continue
		}
		q := &RedisQuery{
			Action:        "ZRANGEBYSCORE",
			Options:       []interface{}{"archive:" + v, start, end},
			resultChannel: make(chan *QueryResult),
		}
		queryservice.queryChannel <- q
		queryresult := <-q.resultChannel
		metricData, err := redis.Strings(queryresult.Value, queryresult.Err)
		if err != nil {
			log.Println(err)
			continue
		}
		record["name"] = v
		record["values"] = GenerateTimeseries(metricData)
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
	for metric, value := range items {
		q := &RedisQuery{
			Action:        "GET",
			Options:       []interface{}{metric},
			resultChannel: make(chan *QueryResult),
		}
		queryservice.queryChannel <- q
		queryresult := <-q.resultChannel
		if queryresult.Value != nil {
			q := &RedisQuery{
				Action:        "HSET",
				Options:       []interface{}{metric, "ttl", value},
				resultChannel: make(chan *QueryResult),
			}
			queryservice.queryChannel <- q
			<-q.resultChannel
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
	q := &RedisQuery{
		Action:        "ZRANGEBYSCORE",
		Options:       []interface{}{"archive:" + metric, start, end},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult := <-q.resultChannel
	metricData, err := redis.Strings(queryresult.Value, queryresult.Err)
	if err != nil {
		log.Println(err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	recordList["name"] = metric
	recordList["url"] = "/api/v1/metric/" + metric
	recordList["records"] = GenerateTimeseries(metricData)
	if body, err := json.Marshal(recordList); err == nil {
		w.Write(body)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// MetricUpdate PATCH /metric/{:name}
func MetricUpdate(w http.ResponseWriter, r *http.Request) {
	metric := mux.Vars(r)["name"]
	var item MetricData
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PATCH, DELETE")
	q := &RedisQuery{
		Action:        "GET",
		Options:       []interface{}{metric},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult := <-q.resultChannel
	if queryresult.Value != nil {
		q := &RedisQuery{
			Action:        "HMSET",
			Options:       []interface{}{metric, "ttl", item.TTL},
			resultChannel: make(chan *QueryResult),
		}
		queryservice.queryChannel <- q
		<-q.resultChannel
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

// MetricDelete DELETE /metric/{:name}
func MetricDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, PATCH, DELETE")
	metric := mux.Vars(r)["name"]

	q := &RedisQuery{
		Action:        "DEL",
		Options:       []interface{}{"archive:" + metric},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult := <-q.resultChannel
	if queryresult.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	q = &RedisQuery{
		Action:        "DEL",
		Options:       []interface{}{metric},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult = <-q.resultChannel
	if queryresult.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
