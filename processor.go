package main

import (
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/garyburd/redigo/redis"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// MetricDeliver define a metric process task
type MetricDeliver struct {
	*redis.Pool
	*Setting
	consumer    *nsq.Consumer
	producer    *nsq.Producer
	exitChannel chan int
	msgChannel  chan *Message
}

func (m *MetricDeliver) Run() error {
	var err error
	dial := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", m.RedisServer)
		if err != nil {
			return nil, err
		}
		return c, err
	}
	m.Pool = redis.NewPool(dial, 3)
	cfg := nsq.NewConfig()
	hostname, err := os.Hostname()
	cfg.Set("user_agent", fmt.Sprintf("metric_processor/%s", hostname))
	cfg.Set("snappy", true)
	cfg.Set("max_in_flight", m.MaxInFlight)
	m.producer, err = nsq.NewProducer(m.NsqdAddress, cfg)
	if err != nil {
		return err
	}
	m.consumer, err = nsq.NewConsumer(m.MetricTopic, m.MetricChannel, cfg)
	if err != nil {
		return err
	}
	m.consumer.AddConcurrentHandlers(m, m.MaxInFlight)
	err = m.consumer.ConnectToNSQLookupds(m.LookupdAddresses)
	if err != nil {
		return err
	}
	go m.writeLoop()
	return err
}

func (m *MetricDeliver) Stop() {
	m.consumer.Stop()
	close(m.exitChannel)
	m.producer.Stop()
	m.Pool.Close()
}

// HandleMessage is MetricDeliver's nsq handle function
func (m *MetricDeliver) HandleMessage(msg *nsq.Message) error {
	message := &Message{
		Body:         msg.Body,
		ErrorChannel: make(chan error),
	}
	m.msgChannel <- message
	if err := <-message.ErrorChannel; err != nil {
		return err
	}
	return nil
}

func (m *MetricDeliver) writeLoop() {
	con := m.Get()
	defer con.Close()
	for {
		select {
		case <-m.exitChannel:
			return
		case msg := <-m.msgChannel:
			metric, ok := msg.Body.([]byte)
			if !ok {
				log.Println("wrong message:", msg.Body)
				msg.ErrorChannel <- nil
				continue
			}
			data := strings.Split(string(metric), " ")
			t, _ := strconv.ParseInt(data[2], 10, 64)
			v, _ := strconv.ParseFloat(data[1], 64)
			record, err := KeyValueEncode(t, v)
			if err != nil {
				log.Println(err)
				continue
			}
			con.Send("APPEND", fmt.Sprintf("archive:%s:%d", data[0], t/14400), record)
			con.Send("HGET", data[0], "atime")
			con.Flush()
			con.Receive()
			t, err = redis.Int64(con.Receive())
			if err != nil && err != redis.ErrNil {
				con.Close()
				con = m.Get()
			}
			if err == redis.ErrNil {
				err = nil
			}
			msg.ErrorChannel <- err
			if (time.Now().Unix()-t*m.MinDuration) > m.MinDuration && err == nil {
				m.producer.Publish(m.ArchiveTopic, []byte(data[0]))
			}
		}
	}
}
