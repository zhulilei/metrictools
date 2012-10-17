package main

import (
	"flag"
	"fmt"
	"github.com/datastream/metrictools"
	"github.com/datastream/metrictools/amqp"
	"github.com/datastream/metrictools/notify"
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
	queue, _ := c.String("alarm", "queue")
	binding_key, _ := c.String("alarm", "bindingkey")
	consumer_tag, _ := c.String("alarm", "consumertag")
	alarm_exchange, _ := c.String("alarm", "alarm_exchange")
	alarm_exchange_type, _ := c.String("alarm", "alarm_exchange_type")
	db_session := metrictools.NewMongo(mongouri, dbname, user, password)
	if db_session == nil {
		log.Println("connect database error")
		os.Exit(1)
	}

	for i := 0; i < nWorker; i++ {
		message_chan := make(chan *amqp.Message)
		notify_chan := make(chan *notify.Notify)
		msg_chan := make(chan []byte)
		consumer := amqp.NewConsumer(uri, exchange, exchange_type, queue, binding_key, consumer_tag)
		producer := amqp.NewProducer(uri, alarm_exchange, alarm_exchange_type, true)
		go consumer.Read_record(message_chan)
		session := db_session.Copy()
		go scan_record(message_chan, notify_chan, session, dbname)
		go notify.Send(notify_chan, msg_chan)
		go dosend(producer, msg_chan)
	}
	select {}
}
func dosend(producer *amqp.Producer, msg_chan chan []byte) {
	producer.Connect_mq()
	for {
		msg := <-msg_chan
		if err := producer.Deliver(msg, ""); err != nil {
			time.Sleep(time.Second * 1)
			go func() {
				msg_chan <- msg
			}()
		}
		if producer.Reliable {
			select {
			case <-producer.Ack:
			case <-producer.Nack:
				go func() {
					time.Sleep(time.Second * 1)
					msg_chan <- msg
				}()
			}
		}
	}
}
