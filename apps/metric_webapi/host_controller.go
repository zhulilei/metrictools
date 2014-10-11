package main

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"sort"
	"strings"
)

// HostIndex GET /host
func (q *WebService) HostIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	reply, _ := q.engine.Do("strings", "SMEMBERS", "hosts:"+user)
	hosts := reply.([]string)
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
func (q *WebService) HostShow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, DELETE")
	host := strings.Replace(mux.Vars(r)["host"], "-", ".", -1)
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	_, err := q.engine.Do("strings", "SMEMBERS", "host:"+user+"_"+host)
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
func (q *WebService) HostDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, DELETE")
	host := strings.Replace(mux.Vars(r)["host"], "-", ".", -1)
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	reply, err := q.engine.Do("strings", "SMEMBERS", "host:"+user+"_"+host)
	metricList := reply.([]string)
	if err == nil {
		var args []interface{}
		for _, v := range metricList {
			args = append(args, v)
			args = append(args, "archive:"+v)
		}
		args = append(args, "host:"+host)
		_, err = q.engine.Do("raw", "DEL", args...)
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

// HostMetricIndex GET /host/{:hostname}/metric
func (q *WebService) HostMetricIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	host := strings.Replace(mux.Vars(r)["host"], "-", ".", -1)
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	reply, err := q.engine.Do("strings", "SMEMBERS", "host:"+user+"_"+host)
	metricList := reply.([]string)
	size := len(user)
	if err == nil {
		var rst []interface{}
		sort.Strings(metricList)
		for _, v := range metricList {
			reply, err := q.engine.Do("int", "HGET", v, "ttl")
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			ttl := reply.(int)
			reply, err = q.engine.Do("string", "HGET", v, "type")
			tp := reply.(string)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			metric := make(map[string]interface{})
			metric["type"] = tp
			metric["ttl"] = ttl
			v = v[size+1:]
			metric["name"] = v
			metric["url"] = "/api/v1/metric/" + v
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
func (q *WebService) HostMetricDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
	host := strings.Replace(mux.Vars(r)["host"], "-", ".", -1)
	metric := mux.Vars(r)["name"]
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	q.engine.Do("raw", "SREM", "host:"+user+"_"+host, metric)
	_, err := q.engine.Do("raw", "DEL", "host:"+user+"_"+host, user+"_"+metric)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
