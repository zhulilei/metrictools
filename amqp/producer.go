package amqp

import (
	"github.com/streadway/amqp"
	"log"
)

type Producer struct {
	amqpURI      string
	exchange     string
	exchangeType string
	Reliable     bool
	channel      *amqp.Channel
	Ack          chan uint64
	Nack         chan uint64
}

func NewProducer(amqpURI, exchange, exchangeType string, reliable bool) *Producer {
	this := &Producer{
		amqpURI:      amqpURI,
		exchange:     exchange,
		exchangeType: exchangeType,
		Reliable:     reliable,
	}
	return this
}

func (this *Producer) Connect_mq() {
	// This function dials, connects, declares, publishes, and tears down,
	// all in one go. In a real service, you probably want to maintain a
	// long-lived connection as state, and publish against that.

	for {
		connection, err := amqp.Dial(this.amqpURI)
		if err != nil {
			log.Println("Dial: ", err)
			continue
		}
		defer connection.Close()
		if this.channel, err = connection.Channel(); err != nil {
			log.Println("Channel: ", err)
			continue
		}
		if err = this.channel.ExchangeDeclare(
			this.exchange,     // name
			this.exchangeType, // type
			true,              // durable
			false,             // auto-deleted
			false,             // internal
			false,             // noWait
			nil,               // arguments
		); err != nil {
			log.Println("Exchange Declare: ", err)
			continue
		}

		// Reliable publisher confirms require confirm.select support from the
		// connection.
		if this.Reliable {
			if err := this.channel.Confirm(false); err != nil {
				log.Println("Channel could not be put into confirm mode: ", err)
				continue
			}
			this.Ack, this.Nack = this.channel.NotifyConfirm(make(chan uint64), make(chan uint64))
		}
		break
	}
}
func (this *Producer) Deliver(body []byte, key string) error {
	var err error
	if err = this.channel.Publish(
		this.exchange,
		key,
		true, // mandatory
		true, // immediate
		amqp.Publishing{
			Headers:         amqp.Table{},
			ContentType:     "text/plain",
			ContentEncoding: "",
			Body:            body,
			DeliveryMode:    amqp.Transient, // 1=non-persistent, 2=persistent
			Priority:        0,              // 0-9
			// a bunch of application/implementation-specific fields
		},
	); err != nil {
		log.Println("Exchange Publish: ", err)
	}
	return err
}
