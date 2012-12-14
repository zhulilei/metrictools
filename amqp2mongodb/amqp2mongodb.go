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

const nWorker = 10

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
	metric_queue, _ := c.String("metric", "queue")
	metric_routing_key, _ := c.String("metric", "routing_key")
	metric_consumer_tag, _ := c.String("metric", "consumer_tag")
	trigger_routing_key, _ := c.String("trigger", "routing_key")
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
	if redis_pool.Get() == nil {
		log.Println("connect redis error")
		os.Exit(1)
	}

	msg_chan := make(chan *amqp.Message)
	redis_notify_chan := make(chan string)
	for i := 0; i < nWorker; i++ {
		consumer := amqp.NewConsumer(uri, exchange, exchange_type, metric_queue, metric_routing_key, metric_consumer_tag)
		// read metric from mq
		go consumer.Read_record(msg_chan)
		// insert metric into mongodb
		go insert_record(msg_chan, db_session, dbname, redis_notify_chan)
	}
	// pusblish data to redis
	go redis_notify(redis_pool, redis_notify_chan)
	// deliver trigger to mq
	producer := amqp.NewProducer(uri, exchange, exchange_type, true)
	deliver_chan := make(chan *amqp.Message)
	go producer.Deliver(deliver_chan)
	// scan trigger, send to mq if trigger (now - last) < 120
	trigger_msg_chan := make(chan []byte)
	go scan_record(db_session, dbname, trigger_msg_chan)
	// pack mq message
	go trigger_dispatch(deliver_chan, trigger_routing_key, trigger_msg_chan)
	// make sure index
	ensure_index(db_session, dbname)
}
