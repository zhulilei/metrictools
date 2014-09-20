package main

import (
	"../.."
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/fzzy/radix/redis"
	"log"
	"os"
)

// DataArchive define data archive task
type DataArchive struct {
	*metrictools.Setting
	consumer    *nsq.Consumer
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
	client, err := redis.Dial(m.Network, m.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
	}
	defer client.Close()
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
			ttl, _ := client.Cmd("HGET", metricName, "ttl").Int64()
			atime, _ := client.Cmd("HGET", metricName, "atime").Int64()
			m1 := fmt.Sprintf("archive:%s:%d", metricName, (atime*m.MinDuration-3600*24)/m.MinDuration)
			m2 := fmt.Sprintf("archive:%s:%d", metricName, (atime*m.MinDuration-3600*24*3)/m.MinDuration)
			m3 := fmt.Sprintf("archive:%s:%d", metricName, (atime*m.MinDuration-3600*24*7)/m.MinDuration)
			reply := client.Cmd("MGET", m1, m2, m3)
			if reply.Err != nil {
				client.Close()
				log.Println("redis connection close")
				client, _ = redis.Dial(m.Network, m.RedisServer)
				msg.ErrorChannel <- reply.Err
				continue
			}
			values, err := reply.List()
			err = compress(m1, []byte(values[0]), atime, ttl-3600*24, 300, client)
			if err != nil {
				msg.ErrorChannel <- err
				continue
			}
			err = compress(m2, []byte(values[1]), atime, ttl-3600*24*3, 600, client)
			if err != nil {
				msg.ErrorChannel <- err
				continue
			}
			err = compress(m3, []byte(values[2]), atime, ttl-3600*24*7, 900, client)
			if err == nil {
				r := client.Cmd("HSET", metricName, "atime", atime+1)
				err = r.Err
			}
			msg.ErrorChannel <- err
		}
	}
}

func compress(metric string, values []byte, atime int64, ttl int64, interval int64, client *redis.Client) error {
	sumvalue := float64(0)
	sumtime := int64(0)
	size := len(values)
	var p metrictools.KeyValue
	var count int
	var data []byte
	if ttl <= 0 {
		if reply := client.Cmd("DEL", metric); reply.Err != nil {
			return reply.Err
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
	client.Append("SET", metric, data)
	client.Append("EXPIRE", metric, ttl)
	client.GetReply()
	reply := client.GetReply()
	return reply.Err
}
