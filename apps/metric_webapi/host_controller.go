package main

import (
	"../.."
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"sort"
)

// HostIndex GET /host
func (q *WebService) HostIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	hosts, _ := q.engine.GetSet(fmt.Sprintf("hosts:%s", user))
	var rst []interface{}
	for _, host := range hosts {
		query := make(map[string]interface{})
		query["name"] = host
		query["metric"] = fmt.Sprintf("/api/v1/host/%s", host)
		rst = append(rst, query)
	}
	body, _ := json.Marshal(rst)
	w.Write(body)
}

// HostShow GET /host/{:name}
func (q *WebService) HostShow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Methods", "GET, DELETE")
	host := mux.Vars(r)["host"]
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	_, err := q.engine.GetSet(fmt.Sprintf("host:%s", string(metrictools.XorBytes([]byte(user), []byte(host)))))
	if err == nil {
		w.WriteHeader(http.StatusOK)
		query := make(map[string]interface{})
		query["name"] = host
		query["metrics_url"] = fmt.Sprintf("/api/v1/host/%s/metric", host)
		body, _ := json.Marshal(query)
		w.Write(body)
	} else {
		w.WriteHeader(http.StatusNotFound)
		log.Println("failed to get set", err)
	}
}

// HostDelete DELETE /host/{:name}
func (q *WebService) HostDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Methods", "GET, DELETE")
	host := mux.Vars(r)["host"]
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	metricList, err := q.engine.GetSet(fmt.Sprintf("host:%s", string(metrictools.XorBytes([]byte(user), []byte(host)))))
	if err == nil {
		var args []interface{}
		for _, v := range metricList {
			args = append(args, v)
		}
		err = q.engine.DeleteData(args...)
	}
	if err == nil {
		err = q.engine.SetDelete(fmt.Sprintf("hosts:%s", user), host)
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
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	host := mux.Vars(r)["host"]
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	metricList, err := q.engine.GetSet(fmt.Sprintf("host:%s", string(metrictools.XorBytes([]byte(user), []byte(host)))))
	if err == nil {
		var rst []interface{}
		sort.Strings(metricList)
		for _, v := range metricList {
			metric, _ := q.engine.GetMetric(v)
			metricHash := make(map[string]interface{})
			metricHash["ttl"] = metric.TTL
			metricHash["name"] = string(metrictools.XorBytes([]byte(user), []byte(v)))
			metricHash["url"] = fmt.Sprintf("/api/v1/metric/%s", metricHash["name"])
			rst = append(rst, metricHash)
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
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
	host := mux.Vars(r)["host"]
	metric := mux.Vars(r)["name"]
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	err := q.engine.DeleteData(metric)
	if err == nil {
		err = q.engine.SetDelete(fmt.Sprintf("host:%s", string(metrictools.XorBytes([]byte(user), []byte(host)))), string(metrictools.XorBytes([]byte(user), []byte(metric))))
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
