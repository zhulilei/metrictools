package main

import (
	"flag"
	"fmt"
	"github.com/datastream/metrictools"
	"github.com/datastream/metrictools/amqp"
	"github.com/kless/goconfig/config"
	"log"
	"os"
	"time"
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
	exchange_type, _ := c.String("Generic", "exchange-type")
	queue, _ := c.String("amqp2mongo", "queue")
	binding_key, _ := c.String("amqp2mongo", "bindingkey")
	consumer_tag, _ := c.String("amqp2mongo", "consumertag")
	alarm_exchange, _ := c.String("alarm", "alarm_exchange")
	alarm_exchange_type, _ := c.String("alarm", "alarm_exchange_type")

	db_session := metrictools.NewMongo(mongouri, dbname, user, password)
	if db_session == nil {
		log.Println("connect database error")
		os.Exit(1)
	}

	msg_chan := make(chan *amqp.Message)
	deliver_chan := make(chan *amqp.Message)
	scan_chan := make(chan *metrictools.Metric)
	notify_chan := make(chan []byte)

	for i := 0; i < nWorker; i++ {
		consumer := amqp.NewConsumer(uri, exchange, exchange_type, queue, binding_key, consumer_tag)
		producer := amqp.NewProducer(uri, alarm_exchange, alarm_exchange_type, true)
		go consumer.Read_record(msg_chan)
		go producer.Deliver(deliver_chan)
		go insert_record(msg_chan, scan_chan, db_session, dbname)
		go scan_record(scan_chan, notify_chan, db_session, dbname)
	}
	go dosend(deliver_chan, notify_chan)
	ensure_index(db_session, dbname)
}

func dosend(deliver_chan chan *amqp.Message, msg_chan chan []byte) {
	for {
		msg_body := <-msg_chan
		msg := &amqp.Message{
			Content: string(msg_body),
			Done:    make(chan int),
			Key:     "",
		}
		deliver_chan <- msg
		go message_retry(deliver_chan, msg)
	}
}

func message_retry(deliver_chan chan *amqp.Message, msg *amqp.Message) {
	for {
		time.Sleep(time.Second * 1)
		stat := <-msg.Done
		if stat < 1 {
			deliver_chan <- msg
		} else {
			break
		}
	}
}
