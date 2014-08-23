package main

import (
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/fzzy/radix/extra/pool"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// MetricDeliver define a metric process task
type MetricDeliver struct {
	*pool.Pool
	*Setting
	consumer    *nsq.Consumer
	producer    *nsq.Producer
	exitChannel chan int
	msgChannel  chan *Message
}

func (m *MetricDeliver) Run() error {
	var err error
	m.Pool, err = pool.NewPool("tcp", m.RedisServer, 5)
	if err != nil {
		return err
	}
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	cfg := nsq.NewConfig()
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
	m.Pool.Empty()
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
	client, err := m.Get()
	if err != nil {
		log.Fatal(err)
	}
	defer m.Put(client)
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
			client.Append("APPEND", fmt.Sprintf("archive:%s:%d", data[0], t/14400), record)
			client.Append("HGET", data[0], "atime")
			reply := client.GetReply()
			if reply.Err != nil {
				client.Close()
				client, _ = m.Get()
			}
			msg.ErrorChannel <- reply.Err
			reply = client.GetReply()
			if reply.Err != nil {
				continue
			} else {
				t, err = reply.Int64()
				err = nil
			}
			if (time.Now().Unix()-t*m.MinDuration) > m.MinDuration && err == nil {
				m.producer.Publish(m.ArchiveTopic, []byte(data[0]))
			}
		}
	}
}
