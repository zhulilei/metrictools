package main

import (
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/garyburd/redigo/redis"
	"log"
	"strconv"
	"strings"
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
	message := &Message{
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
	con := m.Get()
	defer con.Close()
	for {
		select {
		case <-m.exitChannel:
			return
		case msg := <-m.msgChannel:
			metric, ok := msg.Body.([]byte)
			if !ok {
				log.Println("wrong message:", msg.Body)
				msg.ErrorChannel <- nil
				continue
			}
			data := strings.Split(string(metric), " ")
			t, _ := strconv.ParseInt(data[2], 10, 64)
			v, _ := strconv.ParseFloat(data[1], 64)
			record, err := KeyValueEncode(t, v)
			if err != nil {
				log.Println(err)
				continue
			}
			con.Send("APPEND", fmt.Sprintf("archive:%s:%d", data[0], t/14400), record)
			con.Send("HGET", data[0], "atime")
			con.Flush()
			con.Receive()
			t, err = redis.Int64(con.Receive())
			if err != nil && err != redis.ErrNil {
				con.Close()
				con = m.Get()
			}
			if err == redis.ErrNil {
				err = nil
			}
			msg.ErrorChannel <- err
			if (time.Now().Unix()-t*m.MinDuration) > m.MinDuration && err == nil {
				m.writer.Publish(m.ArchiveTopic, []byte(data[0]))
			}
		}
	}
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
