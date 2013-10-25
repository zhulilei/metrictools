package main

import (
	metrictools "../"
	"flag"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

var (
	confFile = flag.String("conf", "metrictools.conf", "analyst config file")
)

var dataService *redis.Pool

func main() {
	flag.Parse()
	c, err := metrictools.ReadConfig(*confFile)
	if err != nil {
		log.Fatal("config parse error", err)
	}
	dataRedisServer, _ := c["redis_server"]
	bind, _ := c["web_bind"]

	dataRedisCon := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", dataRedisServer)
		if err != nil {
			return nil, err
		}
		return c, err
	}
	dataService = redis.NewPool(dataRedisCon, 3)
	defer dataService.Close()

	r := mux.NewRouter()
	s := r.PathPrefix("/api/v1").Subrouter()

	s.HandleFunc("/metric", MetricIndex).
		Methods("GET")

	s.HandleFunc("/metric", MetricCreate).
		Methods("POST").
		Headers("Content-Type", "application/json")

	s.HandleFunc("/metric/{name}", MetricShow).
		Methods("GET")

	s.HandleFunc("/metric/{name}", MetricUpdate).
		Methods("PATCH").
		Headers("Content-Type", "application/json")

	s.HandleFunc("/metric/{name}", MetricDelete).
		Methods("DELETE")

	s.HandleFunc("/host", HostIndex).
		Methods("GET")
	s.HandleFunc("/host/{name}", HostShow).
		Methods("GET")
	s.HandleFunc("/host/{name}", HostDelete).
		Methods("DELETE")

	s.HandleFunc("/host/{host}/metric", HostMetricIndex).
		Methods("GET")
	s.HandleFunc("/host/{host}/metric", MetricUpdate).
		Methods("PATCH").
		Headers("Content-Type", "application/json")
	s.HandleFunc("/host/{host}/metric/{name}", HostMetricDelete).
		Methods("DELETE")

	s.HandleFunc("/trigger", TriggerCreate).
		Methods("POST").
		Headers("Content-Type", "application/json")
	s.HandleFunc("/trigger/{name}", TriggerShow).
		Methods("GET")
	s.HandleFunc("/trigger/{name}", TriggerDelete).
		Methods("DELETE")

	s.HandleFunc("/trigger/{trigger}/action", ActionCreate).
		Methods("POST").Headers("Content-Type", "application/json")
	s.HandleFunc("/trigger/{trigger}/action", ActionIndex).
		Methods("GET")
	s.HandleFunc("/trigger/{trigger}/action/{name}", ActionDelete).
		Methods("DELETE")

	http.Handle("/", r)
	err = http.ListenAndServe(bind, nil)
	if err != nil {
		log.Println(err)
	}
}
