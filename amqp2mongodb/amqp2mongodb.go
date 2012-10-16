package main

import (
	"flag"
	"fmt"
	"github.com/datastream/metrictools/amqp"
	"github.com/datastream/metrictools/mongo"
	"github.com/datastream/metrictools/types"
	"github.com/kless/goconfig/config"
	"os"
)

var (
	conf_file = flag.String("conf", "metrictools.conf", "analyst config file")
)

const nWorker = 10

func main() {
	flag.Parse()
	if len(flag.Args()) != 1 {
		fmt.Printf("Usage: %s n\n\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}
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
	producer := mongo.NewMongo(mongouri, dbname, user, password)

	for i := 0; i < nWorker; i++ {
		message_chan := make(chan *types.Message)
		consumer := amqp.NewConsumer(uri, exchange, exchange_type, queue, binding_key, consumer_tag)
		go consumer.Read_record(message_chan)
		go producer.Insert_record(message_chan)
	}
	select {}
}
