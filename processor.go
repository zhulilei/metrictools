package main

import (
	"encoding/json"
	"github.com/bitly/go-nsq"
	"github.com/garyburd/redigo/redis"
	"log"
	"time"
)

// MetricDeliver define a metric process task
type MetricDeliver struct {
	*redis.Pool
	*Setting
	writer      *nsq.Writer
	reader      *nsq.Reader
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
	m.writer = nsq.NewWriter(m.NsqdAddress)
	m.reader, err = nsq.NewReader(m.MetricTopic, m.MetricChannel)
	if err != nil {
		return err
	}
	m.reader.SetMaxInFlight(m.MaxInFlight)
	for i := 0; i < m.MaxInFlight; i++ {
		m.reader.AddHandler(m)
	}
	for _, addr := range m.LookupdAddresses {
		err := m.reader.ConnectToLookupd(addr)
		if err != nil {
			return err
		}
	}
	go m.writeLoop()
	go m.ScanTrigger()
	return err
}

func (m *MetricDeliver) Stop() {
	m.reader.Stop()
	close(m.exitChannel)
	m.writer.Stop()
	m.Pool.Close()
}

// HandleMessage is MetricDeliver's nsq handle function
func (m *MetricDeliver) HandleMessage(msg *nsq.Message) error {
	var err error
	var c []CollectdJSON
	if err = json.Unmarshal(msg.Body, &c); err != nil {
		log.Println(err)
		return nil
	}
	for _, v := range c {
		if len(v.Values) != len(v.DataSetNames) || len(v.Values) != len(v.DataSetTypes) {
			log.Println("json data error:", string(msg.Body))
			continue
		}
		metrics := v.GenerateMetricData()
		message := &Message{
			Body:         metrics,
			ErrorChannel: make(chan error),
		}
		m.msgChannel <- message
		if err := <-message.ErrorChannel; err != nil {
			return err
		}
	}
	return nil
}

func (m *MetricDeliver) writeLoop() {
	var err error
	con := m.Get()
	defer con.Close()
	for {
		select {
		case <-m.exitChannel:
			return
		case msg := <-m.msgChannel:
			metrics, ok := msg.Body.([]*MetricData)
			if !ok {
				log.Println("wrong message:", msg.Body)
				msg.ErrorChannel <- nil
				continue
			}
			for _, metric := range metrics {
				var nvalue float64
				nvalue, err = getMetricRate(metric, con)
				if err != nil {
					if err.Error() == "ignore" {
						continue
					}
					if err != redis.ErrNil {
						log.Println("fail to get rate value:", err)
						break
					}
				}
				record, err := KeyValueEncode(metric.Timestamp, nvalue)
				if err != nil {
					log.Println(err)
					continue
				}
				metricName := metric.Host + "_" + metric.GetMetricName()
				_, err = con.Do("ZADD", "archive:"+metricName, metric.Timestamp, record)

				if err != nil {
					log.Println("zadd error:", err)
					break
				}
				var t int64
				t, err = redis.Int64(con.Do("HGET", metricName, "archivetime"))
				if err != nil && err != redis.ErrNil {
					log.Println("fail to get archivetime:", err)
					break
				}
				if time.Now().Unix()-t >= 600 {
					m.writer.Publish(m.ArchiveTopic, []byte(metricName))
				}
				_, err = con.Do("HMSET", metricName, "value", metric.Value, "timestamp", metric.Timestamp, "rate_value", nvalue, "dstype", metric.DataSetType, "dsname", metric.DataSetName, "interval", metric.Interval, "host", metric.Host, "plugin", metric.Plugin, "plugin_instance", metric.PluginInstance, "type", metric.Type, "type_instance", metric.TypeInstance)
				if err != nil {
					log.Println("hmset record error:", metricName, err)
					break
				}
				_, err = con.Do("HSETNX", metricName, "ttl", metric.TTL)
				_, err = con.Do("SADD", "host:"+metric.Host, metricName)
			}
			if err != redis.ErrNil {
				con.Close()
				con = m.Get()
			}
			msg.ErrorChannel <- err
		}
	}
}

func getMetricRate(metric *MetricData, con redis.Conn) (float64, error) {
	var value float64
	if metric.DataSetType == "counter" || metric.DataSetType == "derive" {
		rst, err := redis.Values(con.Do("HMGET", metric.Host+"_"+metric.GetMetricName(), "value", "timestamp"))
		if err != nil {
			return 0, err
		}
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
	} else {
		value = metric.Value
	}
	return value, nil
}

// ScanTrigger will find out all trigger which not updated in 60s
func (m *MetricDeliver) ScanTrigger() {
	ticker := time.Tick(time.Second * 30)
	con := m.Get()
	defer con.Close()
	for {
		select {
		case <-ticker:
			keys, err := redis.Strings(con.Do("SMEMBERS", "triggers"))
			if err != nil {
				if err != redis.ErrNil {
					con.Close()
					con = m.Get()
				}
				continue
			}
			now := time.Now().Unix()
			for _, v := range keys {
				last, err := redis.Int64(con.Do("HGET", v, "last"))
				if err != nil && err != redis.ErrNil {
					continue
				}
				if now-last < 61 {
					continue
				}
				_, _, err = m.writer.Publish(m.TriggerTopic, []byte(v))
			}
		case <-m.exitChannel:
			return
		}
	}
}
