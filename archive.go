package main

import (
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/garyburd/redigo/redis"
	"log"
	"os"
)

// DataArchive define data archive task
type DataArchive struct {
	*Setting
	*redis.Pool
	consumer    *nsq.Consumer
	exitChannel chan int
	msgChannel  chan *Message
}

func (m *DataArchive) Run() error {
	var err error
	dial := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", m.RedisServer)
		if err != nil {
			return nil, err
		}
		return c, err
	}
	m.Pool = redis.NewPool(dial, 3)
	hostname, err := os.Hostname()
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
	m.Pool.Close()
}

// HandleMessage is DataArchive's nsq handle function
func (m *DataArchive) HandleMessage(msg *nsq.Message) error {
	message := &Message{
		Body:         string(msg.Body),
		ErrorChannel: make(chan error),
	}
	m.msgChannel <- message
	return <-message.ErrorChannel
}

func (m *DataArchive) archiveData() {
	con := m.Get()
	defer con.Close()
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
			ttl, _ := redis.Int64(con.Do("HGET", metricName, "ttl"))
			atime, _ := redis.Int64(con.Do("HGET", metricName, "atime"))
			m1 := fmt.Sprintf("archive:%s:%d", metricName, (atime*m.MinDuration-3600*24)/m.MinDuration)
			m2 := fmt.Sprintf("archive:%s:%d", metricName, (atime*m.MinDuration-3600*24*3)/m.MinDuration)
			m3 := fmt.Sprintf("archive:%s:%d", metricName, (atime*m.MinDuration-3600*24*7)/m.MinDuration)
			values, err := redis.Strings(con.Do("MGET", m1, m2, m3))
			if err != nil && err != redis.ErrNil {
				msg.ErrorChannel <- err
				continue
			}
			err = compress(m1, []byte(values[0]), atime, ttl-3600*24, 300, con)
			if err != nil {
				msg.ErrorChannel <- err
				continue
			}
			err = compress(m2, []byte(values[1]), atime, ttl-3600*24*3, 600, con)
			if err != nil {
				msg.ErrorChannel <- err
				continue
			}
			err = compress(m3, []byte(values[2]), atime, ttl-3600*24*7, 900, con)
			if err == nil {
				_, err = con.Do("HSET", metricName, "atime", atime+1)
			}
			msg.ErrorChannel <- err
		}
	}
}

func compress(metric string, values []byte, atime int64, ttl int64, interval int64, con redis.Conn) error {
	sumvalue := float64(0)
	sumtime := int64(0)
	size := len(values)
	var p KeyValue
	var count int
	var data []byte
	if ttl <= 0 {
		_, err := con.Do("DEL", metric)
		return err
	}
	for i := 0; i < size; i += 18 {
		if (i + 18) > size {
			break
		}
		kv, _ := KeyValueDecode([]byte(values[i : i+18]))
		offsize := kv.GetTimestamp() - p.GetTimestamp()
		if offsize > interval {
			p = kv
			if count != 0 {
				body, err := KeyValueEncode(sumtime/int64(count), sumvalue/float64(count))
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
	con.Send("SET", metric, data)
	con.Send("EXPIRE", metric, ttl)
	con.Flush()
	con.Receive()
	_, err := con.Receive()
	return err
}
