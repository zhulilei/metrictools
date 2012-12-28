package main

import (
	"flag"
	"github.com/datastream/metrictools"
	"github.com/garyburd/redigo/redis"
	"github.com/kless/goconfig/config"
	"labix.org/v2/mgo"
	"log"
	"net/http"
	"os"
)

var (
	conf_file = flag.String("conf", "metrictools.conf", "analyst config file")
)

const (
	METRIC = 1
	APP    = 2
)

var db_session *mgo.Session
var dbname string
var redis_pool *redis.Pool

func main() {
	flag.Parse()
	c, err := config.ReadDefault(*conf_file)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	mongouri, _ := c.String("Generic", "mongodb")
	dbname, _ = c.String("Generic", "dbname")
	user, _ := c.String("Generic", "user")
	password, _ := c.String("Generic", "password")
	port, _ := c.String("web", "port")
	redis_server, _ := c.String("redis", "server")
	redis_auth, _ := c.String("redis", "password")

	// mongodb
	db_session = metrictools.NewMongo(mongouri, dbname, user, password)
	defer db_session.Close()
	if db_session == nil {
		log.Println("connect database error")
		os.Exit(1)
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

	// get metric data
	http.HandleFunc("/monitorapi/metric", metric_controller)
	// return one host's metric list
	http.HandleFunc("/monitorapi/host_metric", host_metric_controller)
	// clean up one host's metric
	http.HandleFunc("/monitorapi/host_update", host_update_controller)
	// update trigger setting
	http.HandleFunc("/monitorapi/trigger", trigger_controller)

	err = http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Println(err)
	}
}
