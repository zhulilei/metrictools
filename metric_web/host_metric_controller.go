package main

import (
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

func HostHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.WriteHeader(http.StatusOK)
	host := mux.Vars(req)["host"]
	redis_con := redis_pool.Get()
	var query []string
	metric_list, err := redis_con.Do("SMEMBERS", host)
	if err == nil {
		m_list, _ := metric_list.([]interface{})
		for i := range m_list {
			v1, _ := m_list[i].([]byte)
			query = append(query, string(v1))
		}
	} else {
		log.Println("failed to get set", err)
	}
	w.Write(json_host_metric(query, host))
}
