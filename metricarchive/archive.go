package main

import (
	metrictools "../"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/garyburd/redigo/redis"
	"log"
	"time"
)

// DataArchive define data archive task
type DataArchive struct {
	*redis.Pool
	reader              *nsq.Reader
	nsqlookupdAddresses []string
	archiveTopic        string
	archiveChannel      string
	maxInFlight         int
	exitChannel         chan int
	msgChannel          chan *Message
}

func (m *DataArchive) Run() error {
	var err error
	m.reader, err = nsq.NewReader(m.archiveTopic, m.archiveChannel)
	if err != nil {
		return err
	}
	m.reader.SetMaxInFlight(m.maxInFlight)
	for i := 0; i < m.maxInFlight; i++ {
		m.reader.AddHandler(m)
	}
	for _, addr := range m.nsqlookupdAddresses {
		log.Printf("lookupd addr %s", addr)
		err := m.reader.ConnectToLookupd(addr)
		if err != nil {
			return err
		}
	}
	go m.archiveData()
	return err
}

func (m *DataArchive) Stop() {
	m.reader.Stop()
	close(m.exitChannel)
}

// HandleMessage is DataArchive's nsq handle function
func (m *DataArchive) HandleMessage(msg *nsq.Message) error {
	message := &Message{
		body:       string(msg.Body),
		errChannel: make(chan error),
	}
	m.msgChannel <- message
	return <-message.errChannel
}

func (m *DataArchive) archiveData() {
	con := m.Get()
	defer con.Close()
	for {
		select {
		case <-m.exitChannel:
			return
		case msg := <-m.msgChannel:
			metricName, ok := msg.body.(string)
			if !ok {
				log.Println("wrong message:", msg.body)
				return
			}
			stat, _ := redis.Int64(con.Do("HGET", metricName, "ttl"))
			var last int64
			current := time.Now().Unix()
			if stat > 0 {
				last = current - stat
			} else {
				last = current - 300
			}
			if stat == -1 {
				last = current - 60*24*3600
			}
			metric := "archive:" + metricName
			_, err := con.Do("ZREMRANGEBYSCORE", metric, 0, last)
			if err != nil {
				log.Println("failed to remove old data:", metric, err)
				msg.errChannel <- err
			}
			con.Do("HSET", metricName, "archivetime", time.Now().Unix())
			compress(metricName, "5mins", con)
			compress(metricName, "10mins", con)
			compress(metricName, "15mins", con)
		}
	}
}

func compress(metric string, compresstype string, con redis.Conn) error {
	t, err := redis.Int64(con.Do("HGET", metric, compresstype))
	if err != nil && err != redis.ErrNil {
		log.Println("failed to get compress time", err)
		return err
	}
	var interval int64
	now := time.Now().Unix()
	switch compresstype {
	case "5mins":
		if t == 0 {
			t = now - 3600*24*3
		}
		interval = 300
		if t > (now - 3610*24) {
			return nil
		}
	case "10mins":
		if t == 0 {
			t = now - 3600*24*7
		}
		interval = 600
		if t > (now - 3600*24*3) {
			return nil
		}
	case "15mins":
		if t == 0 {
			t = now - 3600*24*15
		}
		interval = 900
		if t > (now - 3600*24*7) {
			return nil
		}
	}
	metricset := "archive:" + metric
	valueList, err := redis.Strings(con.Do("ZRANGEBYSCORE", metricset, t, t+interval))
	if err != nil {
		return err
	}
	sumvalue := float64(0)
	sumtime := int64(0)
	for _, val := range valueList {
		t, v, _ := metrictools.GetTimestampAndValue(val)
		sumvalue += v
		sumtime += t
	}
	size := len(valueList)
	if size > 0 && size != 1 {
		body := fmt.Sprintf("%d:%.2f", sumtime/int64(size), sumvalue/float64(size))
		_, err = con.Do("ZADD", metricset, sumtime/int64(size), body)
		if err != nil {
			return err
		}
		_, err = con.Do("ZREMRANGEBYSCORE", metricset, t, t+interval)
		if err != nil {
			log.Println("failed to remove old data", err)
			return err
		}
	}
	_, err = con.Do("HSET", metric, compresstype, t+interval)
	return err
}

type Message struct {
	body       interface{}
	errChannel chan error
}
