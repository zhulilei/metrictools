package amqp

import (
	"github.com/streadway/amqp"
	"log"
)

type Producer struct {
	amqpURI      string
	exchange     string
	exchangeType string
	reliable     bool
	channel      *amqp.Channel
}

func NewProducer(amqpURI, exchange, exchangeType string, reliable bool) *Producer {
	this := &Producer{
		amqpURI:      amqpURI,
		exchange:     exchange,
		exchangeType: exchangeType,
		reliable:     reliable,
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
		if this.reliable {
			if err := this.channel.Confirm(false); err != nil {
				log.Println("Channel could not be put into confirm mode: ", err)
				continue
			}
			ack, nack := this.channel.NotifyConfirm(make(chan uint64), make(chan uint64))
			defer confirmOne(ack, nack)
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

// One would typically keep a channel of publishings, a sequence number, and a
// set of unacknowledged sequence numbers and loop until the publishing channel
// is closed.
func confirmOne(ack, nack chan uint64) {
	log.Println("waiting for confirmation of one publishing")

	select {
	case tag := <-ack:
		log.Println("confirmed delivery with delivery tag: ", tag)
	case tag := <-nack:
		log.Println("failed delivery of delivery tag: ", tag)
	}
}
