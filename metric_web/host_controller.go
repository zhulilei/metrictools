package main

import (
	"encoding/json"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

func HostHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.WriteHeader(http.StatusOK)
	host := mux.Vars(r)["name"]
	var query []string
	metric_list, err := wb.dataservice.Do("KEYS", "archive:"+host+"*", nil)
	if err == nil {
		m_list, _ := metric_list.([]interface{})
		for i := range m_list {
			v1, _ := m_list[i].([]byte)
			query = append(query, string(v1[8:]))
		}
	} else {
		log.Println("failed to get set", err)
	}
	w.Write(json_host_metric(query, host))
}

func HostClearMetricHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	w.WriteHeader(http.StatusMovedPermanently)

	host := mux.Vars(r)["name"]
	metric_list, err := wb.configservice.Do("SMEMBERS", host, nil)
	if err == nil {
		m_list, _ := metric_list.([]interface{})
		for i := range m_list {
			v1, _ := m_list[i].([]byte)
			wb.dataservice.Do("DEL", string(v1), nil)
			wb.dataservice.Do("DEL", "raw:"+string(v1), nil)
			wb.dataservice.Do("DEL", "archive:"+string(v1), nil)
			wb.configservice.Do("DEL", string(v1), nil)
		}
	}
	_, err = wb.configservice.Do("DEL", host, nil)
	if err != nil {
		log.Println("failed to get set", err)
	}
	w.Header().Set("Location", r.Referer())
}

func HostListMetricHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.WriteHeader(http.StatusOK)
	host := mux.Vars(r)["host"]
	var query []string
	metric_list, err := wb.configservice.Do("SMEMBERS", host, nil)
	if err == nil {
		m_list, _ := metric_list.([]interface{})
		for i := range m_list {
			v1, _ := m_list[i].([]byte)
			query = append(query, string(v1[8:]))
		}
	} else {
		log.Println("failed to get set", err)
	}
	body, _ := json.Marshal(query)
	w.Write(body)
}

func HostDeleteMetricHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	w.WriteHeader(http.StatusOK)
	host := mux.Vars(r)["host"]
	metric := mux.Vars(r)["name"]
	wb.configservice.Do("SREM", host, metric)
	wb.dataservice.Do("DEL", "raw:"+metric, nil)
	wb.dataservice.Do("DEL", "archive:"+metric, nil)
	wb.dataservice.Do("DEL", metric, nil)
	wb.configservice.Do("DEL", metric, nil)
}
