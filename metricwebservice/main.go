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
	conf_file = flag.String("conf", "metrictools.conf", "analyst config file")
)

var dataservice *redis.Pool
var configservice *redis.Pool

func main() {
	flag.Parse()
	c, err := metrictools.ReadConfig(*conf_file)
	if err != nil {
		log.Fatal("config parse error", err)
	}
	config_redis_server, _ := c["config_redis_server"]
	config_redis_auth, _ := c["config_redis_auth"]
	data_redis_server, _ := c["data_redis_server"]
	data_redis_auth, _ := c["data_redis_auth"]
	bind, _ := c["web_bind"]

	// redis
	config_redis_con := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", config_redis_server)
		if err != nil {
			return nil, err
		}
		if _, err := c.Do("AUTH", config_redis_auth); err != nil {
			c.Close()
			return nil, err
		}
		return c, err
	}
	configservice = redis.NewPool(config_redis_con, 3)
	defer configservice.Close()

	data_redis_con := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", data_redis_server)
		if err != nil {
			return nil, err
		}
		if _, err := c.Do("AUTH", data_redis_auth); err != nil {
			c.Close()
			return nil, err
		}
		return c, err
	}
	dataservice = redis.NewPool(data_redis_con, 3)
	defer dataservice.Close()

	r := mux.NewRouter()
	s := r.PathPrefix("/api/v1").Subrouter()

	s.HandleFunc("/metric", MetricIndex).
		Methods("GET")

	s.HandleFunc("/metric", MetricCreate).
		Methods("POST").
		Headers("Content-Type", "application/json")

	s.HandleFunc("/metric/{name}", MetricUpdate).
		Methods("PATCH").
		Headers("Content-Type", "application/json")

	s.HandleFunc("/metric/{name}", MetricDelete).
		Methods("DELETE")

	s.HandleFunc("/host", HostIndex).
		Methods("GET").
		Headers("Accept", "application/json")
	s.HandleFunc("/host/{name}", HostShow).
		Methods("GET").
		Headers("Accept", "application/json")
	s.HandleFunc("/host/{name}", HostDelete).
		Methods("DELETE")

	s.HandleFunc("/host/{host}/metric", HostMetricIndex).
		Methods("GET").
		Headers("Accept", "application/json")
	s.HandleFunc("/host/{host}/metric/{name}", HostMetricDelete).
		Methods("DELETE")

	s.HandleFunc("/statistic/{name}", StatisticShow).
		Methods("GET").
		Headers("Accept", "application/json")

	s.HandleFunc("/trigger", TriggerCreate).
		Methods("POST").
		Headers("Content-Type", "application/json")
	s.HandleFunc("/trigger/{name}", TriggerShow).
		Methods("GET").
		Headers("Accept", "application/json")
	s.HandleFunc("/trigger/{name}", TriggerDelete).
		Methods("DELETE")

	s.HandleFunc("/trigger/{t_name}/action", ActionCreate).
		Methods("POST").Headers("Content-Type", "application/json")
	s.HandleFunc("/trigger/{t_name}/action", ActionIndex).
		Methods("GET").Headers("Accept", "application/json")
	s.HandleFunc("/trigger/{t_name}/action/{name}", ActionDelete).
		Methods("DELETE")

	http.Handle("/", r)
	err = http.ListenAndServe(bind, nil)
	if err != nil {
		log.Println(err)
	}
}
