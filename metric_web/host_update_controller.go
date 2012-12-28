package main

import (
	"io"
	"log"
	"net/http"
)

func host_update_controller(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	w.WriteHeader(http.StatusOK)

	host := req.FormValue("host")
	redis_con := redis_pool.Get()
	var query []string
	var json string
	_, err := redis_con.Do("DEL", host)
	if err == nil {
		io.WriteString(w, "cleanup host")
	} else {
		log.Println("failed to get set", err)
		io.WriteString(w, "internal error")
	}
}
