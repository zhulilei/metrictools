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
	data_con := dataservice.Get()
	defer data_con.Close()
	metric_list, err := data_con.Do("KEYS", "archive:"+host+"*")
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
	config_con := configservice.Get()
	defer config_con.Close()
	metric_list, err := config_con.Do("SMEMBERS", host)
	if err == nil {
		m_list, _ := metric_list.([]interface{})
		data_con := dataservice.Get()
		defer data_con.Close()
		for i := range m_list {
			v1, _ := m_list[i].([]byte)
			data_con.Do("DEL", string(v1))
			data_con.Do("DEL", "raw:"+string(v1))
			data_con.Do("DEL", "archive:"+string(v1))
			data_con.Do("DEL", "setting:"+string(v1))
		}
	}
	_, err = config_con.Do("DEL", host)
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
	config_con := configservice.Get()
	defer config_con.Close()
	metric_list, err := config_con.Do("SMEMBERS", host)
	if err == nil {
		m_list, _ := metric_list.([]interface{})
		for i := range m_list {
			v1, _ := m_list[i].([]byte)
			query = append(query, string(v1))
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
	config_con := configservice.Get()
	defer config_con.Close()
	data_con := dataservice.Get()
	defer data_con.Close()
	config_con.Do("SREM", host, metric)
	data_con.Do("DEL", "raw:"+metric)
	data_con.Do("DEL", "archive:"+metric)
	data_con.Do("DEL", metric)
	config_con.Do("DEL", "setting:"+metric)
}
