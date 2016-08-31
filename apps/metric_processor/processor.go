package main

import (
	"../.."
	"encoding/json"
	"fmt"
	"github.com/afex/hystrix-go/hystrix"
	"github.com/influxdata/influxdb/client/v2"
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
	exitChannel chan int
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
	m.consumer, err = nsq.NewConsumer(m.MetricTopic, m.MetricChannel, cfg)
	if err != nil {
		return err
	}
	m.consumer.AddConcurrentHandlers(m, m.MaxInFlight)
	err = m.consumer.ConnectToNSQLookupds(m.LookupdAddresses)
	hystrix.ConfigureCommand("InsetInfluxdb", hystrix.CommandConfig{
		Timeout:               1000,
		MaxConcurrentRequests: 1000,
		ErrorPercentThreshold: 25,
	})
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
}

// HandleMessage is MetricDeliver's nsq handle function
func (m *MetricDeliver) HandleMessage(msg *nsq.Message) error {
	message := &metrictools.Message{
		Body:         msg.Body,
		ErrorChannel: make(chan error),
	}
	m.msgChannel <- message
	if err := <-message.ErrorChannel; err != nil {
		log.Println(err)
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
	q := client.NewQuery(fmt.Sprintf("CREATE DATABASE %s", m.InfluxdbDatabase), "", "s")
	if response, err := db.Query(q); err != nil {
		if response != nil {
			log.Println(response.Error())
		}
		log.Fatal("create influxdb database failed:", err)
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
			data := string(msg.Body.([]byte))
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
				log.Println("wrong data struct:", string(msg.Body.([]byte)))
				msg.ErrorChannel <- nil
				continue
			}
			for _, c := range dataset {
				timestamp := time.Unix(int64(c.Timestamp), 0)
				fields := make(map[string]interface{})
				tags := m.GenTags(&c, user)
				for i, _ := range c.DataSetNames {
					tags["data_set_type"] = c.DataSetTypes[i]
					fields[c.DataSetNames[i]] = c.Values[i]
				}
				var pt *client.Point
				pt, err = client.NewPoint(c.Plugin, tags, fields, timestamp)
				if err != nil {
					log.Println("NewPoint Error:", err)
					break
				}
				bp.AddPoint(pt)
				if err != nil {
					break
				}
			}
			if err == nil {
				resultChan := make(chan int, 1)
				errChan := hystrix.Go("InsetInfluxdb", func() error {

					err = db.Write(bp)
					if err != nil {
						return err
					}
					resultChan <- 1
					return nil
				}, nil)
				select {
				case <-resultChan:
				case err = <-errChan:
					log.Println("InsetInfluxdb Error", err)
				}
			}
			msg.ErrorChannel <- err
		}
	}
}

func (m *MetricDeliver) GenTags(c *metrictools.CollectdJSON, user string) map[string]string {
	tags := make(map[string]string)
	tags["host"] = c.Host
	tags["owner"] = user
	if len(c.PluginInstance) > 0 {
		tags["plugin_instance"] = c.PluginInstance
	}
	tags["interval"] = fmt.Sprintf("%d", int(c.Interval))
	if len(c.Type) > 0 {
		tags["type"] = c.Type
	}
	if len(c.TypeInstance) > 0 {
		tags["type_instance"] = c.TypeInstance
	}
	return tags
}
