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

type WebService struct {
	dataservice       *metrictools.RedisService
	configservice     *metrictools.RedisService
	config_redis_pool *redis.Pool
}

var wb *WebService

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
	config_redis_pool := redis.NewPool(config_redis_con, 3)
	defer config_redis_pool.Close()

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
	data_redis_pool := redis.NewPool(data_redis_con, 3)
	defer data_redis_pool.Close()

	rs := &metrictools.RedisService{
		RedisPool: data_redis_pool,
		RedisChan: make(chan *metrictools.RedisOP),
	}
	rs2 := &metrictools.RedisService{
		RedisPool: config_redis_pool,
		RedisChan: make(chan *metrictools.RedisOP),
	}
	wb = &WebService{
		dataservice:       rs,
		configservice:     rs2,
		config_redis_pool: config_redis_pool,
	}
	r := mux.NewRouter()
	s := r.PathPrefix("/monitorapi/").Subrouter()

	s.HandleFunc("/metric", MetricHandler).
		Methods("GET").
		Headers("Accept", "application/json")

	s.HandleFunc("/host/{name}", HostHandler).
		Methods("GET").
		Headers("Accept", "application/json")

	s.HandleFunc("/host/{name}/mertic", HostClearMetricHandler).
		Methods("DELETE")

	s.HandleFunc("/statistic/{name}", StatisticHandler).
		Methods("GET").
		Headers("Accept", "application/json")

	s.HandleFunc("/trigger", TriggerNewHandler).
		Methods("POST").
		Headers("Content-type", "application/json")
	s.HandleFunc("/trigger/{name}", TriggerShowHandler).
		Methods("GET").
		Headers("Accept", "application/json")
	s.HandleFunc("/trigger/{name}", TriggerRemoveHandler).
		Methods("DELETE")

	s.HandleFunc("/trigger/{t_name}/action", ActionNewHandler).
		Methods("POST").Headers("Content-type", "application/json")
	s.HandleFunc("/trigger/{t_name}/action", ActionIndexHandler).
		Methods("GET").Headers("Accept", "application/json")
	s.HandleFunc("/trigger/{t_name}/action/{name}", ActionRemoveHandler).
		Methods("DELETE")

	http.Handle("/", r)
	err = http.ListenAndServe(bind, nil)
	if err != nil {
		log.Println(err)
	}
}
