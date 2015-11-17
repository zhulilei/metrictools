package main

import (
	"../.."
	"encoding/json"
	"errors"
	"fmt"
	"github.com/afex/hystrix-go/hystrix"
	"github.com/influxdb/influxdb/client/v2"
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
	cfg.Set("user_agent", fmt.Sprintf("metric_processor-%s/%s", VERSION, hostname))
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
	db, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:      m.InfluxdbAddress,
		Username:  m.InfluxdbUser,
		Password:  m.InfluxdbPassword,
		UserAgent: fmt.Sprintf("metrictools-%s", VERSION),
	})
	if err != nil {
		log.Println("NewHTTPClient error:", err)
	}
	defer db.Close()
	q := client.NewQuery(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", m.InfluxdbDatabase), "", "s")
	if response, err := db.Query(q); err == nil && response.Error() == nil {
		log.Fatal("create influxdb database failed:", response.Results)
	}
	for {
		bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  m.InfluxdbDatabase,
			Precision: "s",
		})
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
			data := string(body)
			index := strings.IndexAny(data, "|")
			if index < 1 {
				log.Println("no user:", data)
				msg.ErrorChannel <- nil
				continue
			}
			user := data[:index]
			var dataset []metrictools.CollectdJSON
			var err error
			err = json.Unmarshal([]byte(data[index+1:]), &dataset)
			if err != nil {
				log.Println("wrong data struct:", msg.Body)
				msg.ErrorChannel <- nil
				continue
			}
			for _, c := range dataset {
				tags := m.GenTags(&c, user)
				fields := make(map[string]interface{})
				for i, _ := range c.DataSetNames {
					fields, err = m.GenFields(&c, i, user)
					if err != nil && err.Error() == "dup data" {
						err = nil
						continue
					}
				}
				if err != nil {
					break
				}
				timestamp := time.Unix(int64(c.Timestamp), 0)
				pt, err := client.NewPoint(c.Plugin, tags, fields, timestamp)
				if err != nil {
					log.Println("NewPoint Error:", err)
					break
				}
				bp.AddPoint(pt)
			}
			if err == nil {
				err = db.Write(bp)
			}
			if err != nil {
				log.Println("write influxdb/redis error:", err)
				time.Sleep(1)
			}
			msg.ErrorChannel <- err
		}
	}
}

func (m *MetricDeliver) GetMetricData(metricName string) (*metrictools.Metric, error) {
	var err error
	resultChan := make(chan metrictools.Metric, 1)
	errChan := hystrix.Go("GetMetric", func() error {
		metric, err := m.engine.GetMetric(metricName)
		if err != nil {
			return err
		}
		resultChan <- metric
		return nil
	}, nil)
	select {
	case metric := <-resultChan:
		return &metric, nil
	case err = <-errChan:
		log.Println("GetMetric Error:", err)
	}
	return nil, err
}

func (m *MetricDeliver) SaveMetricData(metric metrictools.Metric) error {
	resultChan := make(chan int, 1)
	var err error
	errChan := hystrix.Go("SaveMetric", func() error {
		err := m.engine.SaveMetric(metric)
		if err != nil {
			return err
		}
		resultChan <- 1
		return nil
	}, nil)
	select {
	case <-resultChan:
	case err = <-errChan:
		log.Println("SaveMetric Error", err)
	}
	return err
}

func (m *MetricDeliver) GenFields(c *metrictools.CollectdJSON, i int, user string) (map[string]interface{}, error) {
	fields := make(map[string]interface{})
	metricName := c.GetMetricName(i, user)
	metric, err := m.GetMetricData(metricName)
	if err != nil {
		return fields, err
	}
	if metric.LastTimestamp == int64(c.Timestamp) {
		return fields, errors.New("dup data")
	}
	nValue := c.GetMetricRate(metric.LastValue, metric.LastTimestamp, i)
	metric.LastValue = c.Values[i]
	metric.LastTimestamp = int64(c.Timestamp)
	metric.RateValue = nValue
	err = m.SaveMetricData(*metric)
	if err != nil {
		return fields, err
	}
	fields[c.DataSetNames[i]] = nValue
	return fields, err
}

func (m *MetricDeliver) GenTags(c *metrictools.CollectdJSON, user string) map[string]string {
	tags := make(map[string]string)
	tags["host"] = c.Host
	tags["user"] = user
	if len(c.PluginInstance) > 0 {
		tags["plugin_instance"] = c.PluginInstance
	}
	tags["interval"] = fmt.Sprintf("%d", c.Interval)
	if len(c.Type) > 0 {
		tags["type"] = c.Type
	}
	if len(c.TypeInstance) > 0 {
		tags["type_instance"] = c.TypeInstance
	}
	return tags
}
