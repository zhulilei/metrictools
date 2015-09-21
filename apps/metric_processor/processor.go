package main

import (
	"../.."
	"encoding/json"
	"fmt"
	"github.com/nsqio/go-nsq"
	"log"
	"os"
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
	m.engine = &metrictools.RedisEngine{
		Setting:     m.Setting,
		ExitChannel: make(chan int),
		CmdChannel:  make(chan interface{}),
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
	for i := 0; i < m.MaxInFlight; i++ {
		go m.writeLoop()
	}
	taskPool := m.MaxInFlight/100 + 1
	for i := 0; i < taskPool; i++ {
		go m.engine.RunTask()
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
			body, ok := msg.Body.([]byte)
			if !ok {
				log.Println("wrong type:", msg.Body)
				msg.ErrorChannel <- nil
				continue
			}
			data := strings.Split(string(body), "|")
			if len(data) != 2 {
				log.Println("wrong size:", msg.Body)
				msg.ErrorChannel <- nil
				continue
			}
			user := data[0]
			var dataset []metrictools.CollectdJSON
			var err error
			err = json.Unmarshal([]byte(data[1]), &dataset)
			if err != nil {
				log.Println("wrong struct:", msg.Body)
				msg.ErrorChannel <- nil
				continue
			}
			for _, c := range dataset {
				for i := range c.Values {
					metricName := c.GetMetricName(i)
					t := int64(c.Timestamp)
					var metric metrictools.Metric
					metric, err = m.engine.GetMetric(metricName)
					var nValue float64
					if err != nil {
						break
					}
					nValue = c.GetMetricRate(metric.LastValue, metric.LastTimestamp, i)
					record, err := metrictools.KeyValueEncode(t, nValue)
					if err == nil {
						m.engine.SetAttr(metricName, "rate_value", nValue)
						m.engine.SetAttr(metricName, "value", c.Values[i])
						m.engine.SetAttr(metricName, "timestamp", t)
						m.engine.SetAdd(fmt.Sprintf("host:%s:%s", user, c.Host), metricName)
						err = m.engine.AppendKeyValue(fmt.Sprintf("archive:%s:%d", metricName, t/14400), record)
					}
					if err != nil {
						log.Println("insert error", metricName)
						break
					}
					metricInfo, _ := m.engine.GetMetric(metricName)
					t = metricInfo.ArchiveTime
					if (time.Now().Unix()-t*m.MinDuration) > m.MinDuration && err == nil {
						m.producer.Publish(m.ArchiveTopic, []byte(metricName))
					}
					ttl := metricInfo.TTL
					if ttl == 0 {
						ttl = 86400 * 7
					}
					m.engine.SetTTL(fmt.Sprintf("archive:%s:%d", metric, t/14400), ttl)
				}
				if err != nil {
					break
				}
			}
			msg.ErrorChannel <- err
		}
	}
}
