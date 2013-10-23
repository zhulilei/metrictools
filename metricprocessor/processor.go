package main

import (
	metrictools "../"
	"encoding/json"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/garyburd/redigo/redis"
	"log"
	"time"
)

// MetricDeliver define a metric proccess task
type MetricDeliver struct {
	dataService   *redis.Pool
	configService *redis.Pool
	writer        *nsq.Writer
	triggerTopic  string
	archiveTopic  string
	nsqdAddr      string
}

// HandleMessage is MetricDeliver's nsq handle function
func (m *MetricDeliver) HandleMessage(msg *nsq.Message) error {
	var err error
	var c []metrictools.CollectdJSON
	if err = json.Unmarshal(msg.Body, &c); err != nil {
		log.Println(err)
		return nil
	}
	for _, v := range c {
		if len(v.Values) != len(v.DataSetNames) {
			continue
		}
		if len(v.Values) != len(v.DataSetTypes) {
			continue
		}
		metrics := v.GenerateMetricData()
		if err := m.persistData(metrics); err != nil {
			return err
		}
	}
	return nil
}

func (m *MetricDeliver) persistData(metrics []*metrictools.MetricData) error {
	var err error
	dataCon := m.dataService.Get()
	defer dataCon.Close()
	for _, metric := range metrics {
		var nvalue float64
		if metric.DataSetType == "counter" || metric.DataSetType == "derive" {
			nvalue, err = m.getRate(metric)
		} else {
			nvalue = metric.Value
		}
		if err != nil {
			if err.Error() == "ignore" {
				continue
			}
			if err != redis.ErrNil {
				log.Println("fail to get new value", err)
				break
			}
		}
		record := fmt.Sprintf("%d:%.2f", metric.Timestamp, nvalue)
		metricName := metric.Host + "_" + metric.GetMetricName()
		_, err = dataCon.Do("ZADD", "archive:"+metricName, metric.Timestamp, record)
		if err != nil {
			log.Println(err)
			break
		}
		var t int64
		t, err = redis.Int64(dataCon.Do("HGET", metricName, "archivetime"))
		if err != nil && err != redis.ErrNil {
			log.Println("fail to get archivetime", err)
			break
		}
		if time.Now().Unix()-t >= 600 {
			m.writer.Publish(m.archiveTopic, []byte(metricName))
		}
		_, err = dataCon.Do("HMSET", metricName, "value", metric.Value, "timestamp", metric.Timestamp, "rate_value", nvalue, "dstype", metric.DataSetType, "dsname", metric.DataSetName, "interval", metric.Interval, "host", metric.Host, "plugin", metric.Plugin, "plugin_instance", metric.PluginInstance, "type", metric.Type, "type_instance", metric.TypeInstance)
		if err != nil {
			log.Println("hmset", metricName, err)
			break
		}
		_, err = dataCon.Do("HSETNX", metricName, "ttl", metric.TTL)
		_, err = dataCon.Do("SADD", "host:"+metric.Host, metricName)
	}
	return err
}

func (m *MetricDeliver) getRate(metric *metrictools.MetricData) (float64, error) {
	dataCon := m.dataService.Get()
	defer dataCon.Close()
	rst, err := redis.Values(dataCon.Do("HMGET", metric.Host+"_"+metric.GetMetricName(), "value", "timestamp"))
	if err != nil {
		return 0, err
	}
	var value float64
	var t int64
	var v float64
	_, err = redis.Scan(rst, &v, &t)
	if err == nil {
		value = (metric.Value - v) / float64(metric.Timestamp-t)
	} else {
		value = metric.Value
	}
	if value < 0 {
		value = 0
	}
	return value, nil
}

// ScanTrigger will find out all trigger which not updated in 60s
func (m *MetricDeliver) ScanTrigger() {
	ticker := time.Tick(time.Second * 30)
	configCon := m.configService.Get()
	defer configCon.Close()
	for {
		keys, err := redis.Strings(configCon.Do("KEYS", "trigger:*"))
		if err != nil {
			continue
		}
		now := time.Now().Unix()
		for _, v := range keys {
			last, err := redis.Int64(configCon.Do("HGET", v, "last"))
			if err != nil && err != redis.ErrNil {
				continue
			}
			if now-last < 61 {
				continue
			}
			_, _, err = m.writer.Publish(m.triggerTopic, []byte(v))
		}
		<-ticker
	}
}
