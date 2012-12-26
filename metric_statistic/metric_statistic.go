package main

import (
	"flag"
	"fmt"
	"github.com/datastream/metrictools"
	"github.com/datastream/metrictools/amqp"
	"github.com/datastream/metrictools/notify"
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
	trigger_routing_key, _ := c.String("trigger", "routing_key")
	trigger_queue, _ := c.String("trigger", "queue")
	trigger_consumer_tag, _ := c.String("trigger", "consumer_tag")
	notify_routing_key, _ := c.String("notify", "routing_key")
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
	// get trigger
	trigger_chan := make(chan *amqp.Message)
	trigger_consumer := amqp.NewConsumer(uri, exchange, exchange_type, trigger_queue, trigger_routing_key, trigger_consumer_tag)
	// read trigger from mq
	go trigger_consumer.Read_record(trigger_chan)
	// setup channel
	cal_chan := make(chan string)
	update_chan := make(chan string)
	// dispatch trigger msg
	go trigger_chan_dispatch(trigger_chan, update_chan, cal_chan)
	// update trigger's last modify time in mongodb
	go update_all_trigger(db_session, dbname, update_chan)
	// redis data calculate
	notify_chan := make(chan *notify.Notify)
	go calculate_trigger(redis_pool, db_session, dbname, cal_chan, notify_chan)

	deliver_chan := make(chan *amqp.Message)
	// pack up notify message to amqp message
	go deliver_notify(notify_chan, deliver_chan, notify_routing_key)
	// deliver amqp.message
	producer := amqp.NewProducer(uri, exchange, exchange_type, true)
	producer.Deliver(deliver_chan)
}
