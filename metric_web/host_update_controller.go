package main

import (
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

func HostUpdateHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	w.WriteHeader(http.StatusMovedPermanently)

	host := mux.Vars(req)["host"]
	redis_con := redis_pool.Get()
	_, err := redis_con.Do("DEL", host)
	if err != nil {
		log.Println("failed to get set", err)
	}
	w.Header().Set("Location", req.Referer())
}
