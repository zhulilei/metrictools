package main

import (
	"../.."
	"fmt"
	"github.com/bitly/go-nsq"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// MetricDeliver define a metric process task
type MetricDeliver struct {
	*metrictools.Setting
	consumer    *nsq.Consumer
	producer    *nsq.Producer
	exitChannel chan int
	engine      metrictools.StoreEngine
	msgChannel  chan *metrictools.Message
}

func (m *MetricDeliver) Run() error {
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
	m.engine = &metrictools.RedisEngine{Setting: m.Setting}
	go m.engine.Start()
	m.consumer, err = nsq.NewConsumer(m.MetricTopic, m.MetricChannel, cfg)
	if err != nil {
		return err
	}
	m.consumer.AddConcurrentHandlers(m, m.MaxInFlight)
	err = m.consumer.ConnectToNSQLookupds(m.LookupdAddresses)
	if err != nil {
		return err
	}
	for i := 0; i < m.MaxInFlight; i++ {
		go m.writeLoop()
	}
	return err
}

func (m *MetricDeliver) Stop() {
	m.consumer.Stop()
	close(m.exitChannel)
	m.engine.Stop()
	m.producer.Stop()
}

// HandleMessage is MetricDeliver's nsq handle function
func (m *MetricDeliver) HandleMessage(msg *nsq.Message) error {
	message := &metrictools.Message{
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
			record, err := metrictools.KeyValueEncode(t, v)
			if err != nil {
				log.Println(err)
				continue
			}
			tokens := strings.Split(data[0], "_")
			user := tokens[0]
			host := tokens[1]
			err = m.engine.SetAdd("host:"+user+"_"+host, data[0])
			if err == nil {
				err = m.engine.AppendKeyValue(fmt.Sprintf("archive:%s:%d", data[0], t/14400), record)
			}
			msg.ErrorChannel <- err
			if err == nil {
				metricInfo, err := m.engine.GetMetric(data[0])
				t = metricInfo.ArchiveTime
				if (time.Now().Unix()-t*m.MinDuration) > m.MinDuration && err == nil {
					m.producer.Publish(m.ArchiveTopic, []byte(data[0]))
				}
			}
		}
	}
}
