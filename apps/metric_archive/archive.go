package main

import (
	"../.."
	"fmt"
	"github.com/nsqio/go-nsq"
	"log"
	"os"
)

// DataArchive define data archive task
type DataArchive struct {
	*metrictools.Setting
	consumer    *nsq.Consumer
	engine      metrictools.StoreEngine
	exitChannel chan int
	msgChannel  chan *metrictools.Message
}

func (m *DataArchive) Run() error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	cfg := nsq.NewConfig()
	cfg.Set("user_agent", fmt.Sprintf("metric_archive/%s", hostname))
	cfg.Set("snappy", true)
	cfg.Set("max_in_flight", m.MaxInFlight)
	m.consumer, err = nsq.NewConsumer(m.ArchiveTopic, m.ArchiveChannel, cfg)
	if err != nil {
		return err
	}
	m.engine = &metrictools.RedisEngine{
		Setting:     m.Setting,
		ExitChannel: make(chan int),
		CmdChannel:  make(chan interface{}),
	}
	go m.engine.RunTask()
	m.consumer.AddConcurrentHandlers(m, m.MaxInFlight)
	err = m.consumer.ConnectToNSQLookupds(m.LookupdAddresses)
	if err != nil {
		return err
	}
	go m.archiveData()
	return err
}

func (m *DataArchive) Stop() {
	m.consumer.Stop()
	close(m.exitChannel)
	m.engine.Stop()
}

// HandleMessage is DataArchive's nsq handle function
func (m *DataArchive) HandleMessage(msg *nsq.Message) error {
	message := &metrictools.Message{
		Body:         string(msg.Body),
		ErrorChannel: make(chan error),
	}
	m.msgChannel <- message
	return <-message.ErrorChannel
}

func (m *DataArchive) archiveData() {
	for {
		select {
		case <-m.exitChannel:
			return
		case msg := <-m.msgChannel:
			metricName, ok := msg.Body.(string)
			if !ok {
				log.Println("wrong message:", msg.Body)
				msg.ErrorChannel <- nil
				continue
			}
			metricInfo, err := m.engine.GetMetric(metricName)
			atime := metricInfo.ArchiveTime
			ttl := metricInfo.TTL
			if ttl < 86400*7 {
				msg.ErrorChannel <- nil
				continue
			}
			m1 := fmt.Sprintf("arc:%s:%d", metricName, (atime*m.MinDuration-3600*24)/m.MinDuration)
			m2 := fmt.Sprintf("arc:%s:%d", metricName, (atime*m.MinDuration-3600*24*3)/m.MinDuration)
			m3 := fmt.Sprintf("arc:%s:%d", metricName, (atime*m.MinDuration-3600*24*7)/m.MinDuration)
			values, err := m.engine.GetValues(m1, m2, m3)
			if err != nil {
				msg.ErrorChannel <- err
				continue
			}
			err = m.compress(m1, []byte(values[0]), atime, ttl-3600*24, 300)
			if err != nil {
				msg.ErrorChannel <- err
				continue
			}
			err = m.compress(m2, []byte(values[1]), atime, ttl-3600*24*3, 600)
			if err != nil {
				msg.ErrorChannel <- err
				continue
			}
			err = m.compress(m3, []byte(values[2]), atime, ttl-3600*24*7, 900)
			if err == nil {
				err = m.engine.SetAttr(metricName, "atime", atime+1)
			}
			msg.ErrorChannel <- err
		}
	}
}

func (m *DataArchive) compress(metric string, values []byte, atime int64, ttl int64, interval int64) error {
	sumvalue := float64(0)
	sumtime := int64(0)
	size := len(values)
	var p metrictools.KeyValue
	var count int
	var data []byte
	if ttl <= 0 {
		if err := m.engine.DeleteData(metric); err != nil {
			return err
		}
	}
	for i := 0; i < size; i += 18 {
		if (i + 18) > size {
			break
		}
		kv, _ := metrictools.KeyValueDecode([]byte(values[i : i+18]))
		offsize := kv.GetTimestamp() - p.GetTimestamp()
		if offsize > interval {
			p = kv
			if count != 0 {
				body, err := metrictools.KeyValueEncode(sumtime/int64(count), sumvalue/float64(count))
				if err != nil {
					return err
				}
				count = 0
				sumvalue = 0
				sumtime = 0
				data = append(data, body...)
			}
		}
		sumvalue += kv.GetValue()
		sumtime += kv.GetTimestamp()
		count++
	}
	if len(data) > 0 {
		m.engine.SetKeyValue(metric, data)
		err := m.engine.SetTTL(metric, ttl)
		return err
	}
	return nil
}
