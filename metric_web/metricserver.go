package main

import (
	metrictools "../"
	"flag"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"labix.org/v2/mgo"
	"log"
	"net/http"
)

var (
	conf_file = flag.String("conf", "metrictools.conf", "analyst config file")
)

var db_session *mgo.Session
var dbname string
var redis_pool *redis.Pool
var metric_collection string
var trigger_collection string
var statistic_collection string
var notify_collection string

func main() {
	flag.Parse()
	c, err := metrictools.ReadConfig(*conf_file)
	if err != nil {
		log.Fatal("config parse error", err)
	}
	mongouri, _ := c.Global["mongodb"]
	dbname, _ = c.Global["dbname"]
	user, _ := c.Global["user"]
	password, _ := c.Global["password"]
	redis_server, _ := c.Redis["server"]
	redis_auth, _ := c.Redis["auth"]
	metric_collection, _ = c.Metric["collection"]
	trigger_collection, _ = c.Trigger["collection"]
	statistic_collection, _ = c.Statistic["collection"]
	notify_collection, _ = c.Notify["collection"]
	bind, _ := c.Web["bind"]

	// mongodb
	db_session = metrictools.NewMongo(mongouri, dbname, user, password)
	defer db_session.Close()
	if db_session == nil {
		log.Fatal("connect database error")
	}
	// redis
	redis_con := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", redis_server)
		if err != nil {
			return nil, err
		}
		if _, err := c.Do("AUTH", redis_auth); err != nil {
			c.Close()
			return nil, err
		}
		return c, err
	}
	redis_pool = redis.NewPool(redis_con, 3)
	defer redis_pool.Close()
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
	s.HandleFunc("/trigger/{name}", TriggerUpdateHandler).
		Methods("PUT")
	s.HandleFunc("/trigger/{name}", TriggerShowHandler).
		Methods("GET").
		Headers("Accept", "application/json")
	s.HandleFunc("/trigger/{name}", TriggerRemoveHandler).
		Methods("DELETE")
	http.Handle("/", r)
	err = http.ListenAndServe(bind, nil)
	if err != nil {
		log.Println(err)
	}
}
