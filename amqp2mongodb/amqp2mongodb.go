package main

import (
	"flag"
	"fmt"
	"github.com/datastream/metrictools"
	"github.com/datastream/metrictools/amqp"
	"github.com/kless/goconfig/config"
	"github.com/garyburd/redigo/redis"
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
	queue, _ := c.String("amqp2mongo", "queue")
	binding_key, _ := c.String("amqp2mongo", "bindingkey")
	consumer_tag, _ := c.String("amqp2mongo", "consumertag")
	alarm_exchange, _ := c.String("alarm", "alarm_exchange")
	alarm_exchange_type, _ := c.String("alarm", "alarm_exchange_type")
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
	redis_con := func()(redis.Conn, error) {
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
	deliver_chan := make(chan *amqp.Message)

	for i := 0; i < nWorker; i++ {
		consumer := amqp.NewConsumer(uri, exchange, exchange_type, queue, binding_key, consumer_tag)
		producer := amqp.NewProducer(uri, alarm_exchange, alarm_exchange_type, true)
		go consumer.Read_record(msg_chan)
		go producer.Deliver(deliver_chan)
		go insert_record(msg_chan, db_session, dbname, redis_pool)
	}
	ensure_index(db_session, dbname)
}
