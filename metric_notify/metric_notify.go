package main

import (
	"flag"
	"fmt"
	"github.com/datastream/metrictools"
	"github.com/datastream/metrictools/amqp"
	"github.com/garyburd/redigo/redis"
	"github.com/kless/goconfig/config"
	"log"
	"os"
)

var (
	conf_file = flag.String("conf", "metrictools.conf", "analyst config file")
)

func main() {
	flag.Parse()
	c, err := config.ReadDefault(*conf_file)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	mongouri, _ := c.String("Generic", "mongodb")
	dbname, _ := c.String("Generic", "dbname")
	user, _ := c.String("Generic", "user")
	password, _ := c.String("Generic", "password")
	uri, _ := c.String("Generic", "amqpuri")
	exchange, _ := c.String("Generic", "exchange")
	exchange_type, _ := c.String("Generic", "exchange_type")
	notify_routing_key, _ := c.String("notify", "routing_key")
	notify_queue, _ := c.String("notify", "queue")
	notify_consumer_tag, _ := c.String("notify", "consumer_tag")
	redis_server, _ := c.String("redis", "server")
	redis_auth, _ := c.String("redis", "password")

	// mongodb
	db_session := metrictools.NewMongo(mongouri, dbname, user, password)
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
	redis_pool := redis.NewPool(redis_con, 3)
	defer redis_pool.Close()
	// get notify
	notify_chan := make(chan *amqp.Message)
	notify_consumer := amqp.NewConsumer(uri, exchange, exchange_type, notify_queue, notify_routing_key, notify_consumer_tag)
	// read notify from mq
	go notify_consumer.Read_record(notify_chan)
	// do notify
	do_notify(db_session, dbname, notify_chan)
}
