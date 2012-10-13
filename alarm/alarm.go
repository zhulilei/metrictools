package main

import (
	"flag"
	"github.com/datastream/metrictools/amqp"
	"github.com/datastream/metrictools/mongo"
	"github.com/datastream/metrictools/notify"
	"github.com/datastream/metrictools/types"
	"time"
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
	db := mongo.NewMongo(*mongouri, *dbname, *user, *password)
	for i := 0; i < nWorker; i++ {
		message_chan := make(chan *types.Message)
		notify_chan := make(chan *notify.Notify)
		msg_chan := make(chan []byte)
		consumer := amqp.NewConsumer(*uri, *exchange, *exchangeType, *queue, *bindingKey, *consumerTag)
		producer := amqp.NewProducer(*uri, "alarm_message", "topic", true)
		go consumer.Read_record(message_chan)
		go db.Scan_record(message_chan, notify_chan)
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
