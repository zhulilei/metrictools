package main

import (
	"io"
	"log"
	"net/http"
)

func host_metric_controller(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	w.WriteHeader(http.StatusOK)

	host := req.FormValue("host")
	redis_con := redis_pool.Get()
	var query []string
	var json string
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

	json = json_host_type(query, host)
	if len(json) > 0 {
		io.WriteString(w, "["+json+"]")
	} else {
		io.WriteString(w, "internal error")
	}
}
