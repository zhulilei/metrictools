package main

import (
	metrictools "../"
	"encoding/json"
	"fmt"
	"github.com/bitly/nsq/nsq"
	"github.com/garyburd/redigo/redis"
	"log"
	"time"
)

type MetricDeliver struct {
	dataservice   *redis.Pool
	configservice *redis.Pool
	writer        *nsq.Writer
	trigger_topic string
	archive_topic string
	nsqd_addr     string
}

func (this *MetricDeliver) HandleMessage(m *nsq.Message) error {
	var err error
	var c []metrictools.CollectdJSON
	if err = json.Unmarshal(m.Body, &c); err != nil {
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
		if err := this.PersistData(metrics); err != nil {
			return err
		}
	}
	return nil
}

func (this *MetricDeliver) PersistData(metrics []*metrictools.MetricData) error {
	var err error
	data_con := this.dataservice.Get()
	defer data_con.Close()
	for _, metric := range metrics {
		var new_value float64
		if metric.DataSetType == "counter" || metric.DataSetType == "derive" {
			new_value, err = this.getRate(metric)
		} else {
			new_value = metric.Value
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
		record := fmt.Sprintf("%d:%.2f", metric.Timestamp, new_value)
		metric_name := metric.Host + "_" + metric.GetMetricName()
		_, err = data_con.Do("ZADD", "archive:"+metric_name, metric.Timestamp, record)
		if err != nil {
			log.Println(err)
			break
		}
		var t int
		t, err = redis.Int64(data_con.Do("HGET", metric_name, "archivetime"))
		if err != nil && err != redis.ErrNil {
			log.Println("fail to get archivetime", err)
			break
		}
		if time.Now().Unix()-t >= 600 {
			this.writer.Publish(this.archive_topic, []byte(metric_name))
		}
		_, err = data_con.Do("HMSET", metric_name, "value", metric.Value, "timestamp", metric.Timestamp, "rate_value", new_value, "dstype", metric.DataSetType, "dsname", metric.DataSetName, "interval", metric.Interval, "host", metric.Host, "plugin", metric.Plugin, "plugin_instance", metric.PluginInstance, "type", metric.Type, "type_instance", metric.TypeInstance)
		if err != nil {
			log.Println("hmset", metric_name, err)
			break
		}
		_, err = data_con.Do("HSETNX", metric_name, "ttl", metric.TTL)
		_, err = data_con.Do("SADD", metric.Host, metric_name)
	}
	return err
}

func (this *MetricDeliver) getRate(metric *metrictools.MetricData) (float64, error) {
	data_con := this.dataservice.Get()
	defer data_con.Close()
	rst, err := redis.Values(data_con.Do("HMGET", metric.Host+"_"+metric.GetMetricName(), "value", "timestamp"))
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

func (this *MetricDeliver) ScanTrigger() {
	ticker := time.Tick(time.Minute)
	config_con := this.configservice.Get()
	defer config_con.Close()
	for {
		keys, err := redis.Strings(config_con.Do("KEYS", "trigger:*"))
		if err != nil {
			continue
		}
		for _, v := range keys {
			last, err := redis.Int64(config_con.Do("HGET", v, "last"))
			if err != nil {
				continue
			}
			now := time.Now().Unix()
			if now-last < 61 {
				continue
			}
			_, _, err = this.writer.Publish(this.trigger_topic, []byte(v))
		}
		<-ticker
	}
}
