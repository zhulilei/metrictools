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

// HostIndex GET /host
func HostIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	q := &RedisQuery{
		Action:        "SMEMBERS",
		Options:       []interface{}{"hosts"},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult := <-q.resultChannel
	hosts, _ := redis.Strings(queryresult.Value, queryresult.Err)
	var rst []interface{}
	for _, host := range hosts {
		query := make(map[string]interface{})
		query["name"] = host
		query["metric"] = "/api/v1/host/" + host
		rst = append(rst, query)
	}
	body, _ := json.Marshal(rst)
	w.Write(body)
}

// HostShow GET /host/{:name}
func HostShow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, DELETE")
	host := strings.Replace(mux.Vars(r)["host"], "-", ".", -1)
	q := &RedisQuery{
		Action:        "SMEMBERS",
		Options:       []interface{}{"host:" + host},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult := <-q.resultChannel
	_, err := redis.Strings(queryresult.Value, queryresult.Err)
	if err == nil {
		w.WriteHeader(http.StatusOK)
		query := make(map[string]interface{})
		query["name"] = host
		query["metrics_url"] = "/api/v1/host/" + host + "/metric"
		body, _ := json.Marshal(query)
		w.Write(body)
	} else {
		w.WriteHeader(http.StatusNotFound)
		log.Println("failed to get set", err)
	}
}

// HostDelete DELETE /host/{:name}
func HostDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, DELETE")
	host := strings.Replace(mux.Vars(r)["host"], "-", ".", -1)
	q := &RedisQuery{
		Action:        "SMEMBERS",
		Options:       []interface{}{"host:" + host},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult := <-q.resultChannel
	metricList, err := redis.Strings(queryresult.Value, queryresult.Err)
	if err == nil {
		for _, v := range metricList {
			q := &RedisQuery{
				Action:        "DEL",
				Options:       []interface{}{v},
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
				Options:       []interface{}{"archive:" + v},
				resultChannel: make(chan *QueryResult),
			}
			queryservice.queryChannel <- q
			queryresult = <-q.resultChannel
			if queryresult.Err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		q = &RedisQuery{
			Action:        "DEL",
			Options:       []interface{}{"host:" + host},
			resultChannel: make(chan *QueryResult),
		}
		queryservice.queryChannel <- q
		queryresult = <-q.resultChannel
		if queryresult.Err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

// HostMetricIndex GET /host/{:hostname}/metric
func HostMetricIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	host := strings.Replace(mux.Vars(r)["host"], "-", ".", -1)
	q := &RedisQuery{
		Action:        "SMEMBERS",
		Options:       []interface{}{"host:" + host},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult := <-q.resultChannel
	metricList, err := redis.Strings(queryresult.Value, queryresult.Err)
	if err == nil {
		var rst []interface{}
		sort.Strings(metricList)
		for _, v := range metricList {
			q = &RedisQuery{
				Action:        "HMGET",
				Options:       []interface{}{v, "dstype", "dsname", "interval", "host", "plugin", "plugin_instance", "type", "type_instance", "ttl"},
				resultChannel: make(chan *QueryResult),
			}
			queryservice.queryChannel <- q
			queryresult = <-q.resultChannel

			m, err := redis.Values(queryresult.Value, queryresult.Err)
			if err != nil {
				log.Println("failed to hgetall", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			var item metrictools.MetricData
			_, err = redis.Scan(m, &item.DataSetType, &item.DataSetName, &item.Interval, &item.Host, &item.Plugin, &item.PluginInstance, &item.Type, &item.TypeInstance, &item.TTL)
			if err != nil {
				log.Println("failed to scanstruct", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			metric := make(map[string]interface{})
			name := item.Host + "_" + item.GetMetricName()
			metric["name"] = name
			metric["host"] = item.Host
			metric["plugin"] = item.Plugin
			metric["plugin_instance"] = item.PluginInstance
			metric["type"] = item.Type
			metric["type_instance"] = item.TypeInstance
			metric["interval"] = item.Interval
			metric["dsname"] = item.DataSetName
			metric["dstype"] = item.DataSetType
			metric["ttl"] = item.TTL
			metric["url"] = "/api/v1/metric/" + name
			rst = append(rst, metric)
		}
		if body, err := json.Marshal(rst); err == nil {
			w.Write(body)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

// HostMetricDelete DELETE /host/{:hostname}/metric/{:name}
func HostMetricDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
	host := strings.Replace(mux.Vars(r)["host"], "-", ".", -1)
	metric := mux.Vars(r)["name"]
	q := &RedisQuery{
		Action:        "SREM",
		Options:       []interface{}{"host:" + host, metric},
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
		Options:       []interface{}{"archive:" + metric},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult = <-q.resultChannel
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
	w.WriteHeader(http.StatusOK)
}
