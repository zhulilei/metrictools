package main

import (
	"flag"
	"github.com/datastream/metrictools/amqp"
	"github.com/datastream/metrictools/mongo"
	"github.com/datastream/metrictools/types"
)

var (
	uri          = flag.String("uri", "amqp://guest:guest@localhost:5672/", "AMQP URI")
	exchange     = flag.String("exchange", "test-exchange", "Durable AMQP exchange name")
	exchangeType = flag.String("exchange-type", "direct", "Exchange type - direct|fanout|topic|x-custom")
	queue        = flag.String("queue", "test-queue", "Ephemeral AMQP queue name")
	bindingKey   = flag.String("key", "test-key", "AMQP binding key")
	consumerTag  = flag.String("consumer-tag", "simple-consumer", "AMQP consumer tag (should not be blank)")
	mongouri     = flag.String("mongouri", "mongodb://myuser:mypass@localhost:27017/mydatabase", "MONGODB RUI")
	user         = flag.String("user", "admin", "mongodb user")
	password     = flag.String("passwd", "admin", "mongodb password")
	dbname       = flag.String("db", "mydatabase", "mongodb database")
)

const nWorker = 10

func main() {
	flag.Parse()
	producer := mongo.NewMongo(*mongouri, *dbname, *user, *password)
	for i := 0; i < nWorker; i++ {
		message_chan := make(chan *types.Message)
		consumer := amqp.NewConsumer(*uri, *exchange, *exchangeType, *queue, *bindingKey, *consumerTag)
		go consumer.Read_record(message_chan)
		go producer.Scan_record(message_chan)
	}
	select {}
}
