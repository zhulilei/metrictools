package metrictools

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/datastream/nsq/nsq"
	"github.com/garyburd/redigo/redis"
	"labix.org/v2/mgo"
	"log"
	"strconv"
	"strings"
)

type Message struct {
	*nsq.Message
	ResponseChannel chan *nsq.FinishedMessage
}

type MsgDeliver struct {
	MessageChan    chan *Message
	MSession       *mgo.Session
	DBName         string
	RedisChan      chan *RedisOP
	RedisPool      *redis.Pool
	VerboseLogging bool
}

type RedisOP struct {
	Action string
	Key    string
	Value  interface{}
	Result interface{}
	Err    error
	Done   chan int
}

func (this *MsgDeliver) ParseJSON(c CollectdJSON) []*Record {
	keys := c.GenNames()
	var msgs []*Record
	for i := range c.Values {
		msg := &Record{
			Host:      c.Host,
			Key:       c.Host + "_" + keys[i],
			Value:     c.Values[i],
			Timestamp: int64(c.TimeStamp),
			TTL:       int(c.Interval) * 3 / 2,
			DSType:    c.DSTypes[i],
			Interval:  c.Interval,
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

func (this *MsgDeliver) HandleMessage(m *nsq.Message, r chan *nsq.FinishedMessage) {
	this.MessageChan <- &Message{m, r}
}

func (this *MsgDeliver) ProcessData() {
	for {
		m := <-this.MessageChan
		go this.insert_data(m)
	}
}

func (this *MsgDeliver) insert_data(m *Message) {
	var err error
	var c []CollectdJSON
	if err = json.Unmarshal(m.Body, &c); err != nil {
		m.ResponseChannel <- &nsq.FinishedMessage{
			m.Id, 0, true}
		log.Println(err)
		return
	}
	if this.VerboseLogging {
		log.Println("RAW JSON String: ", string(m.Body))
		log.Println("JSON SIZE: ", len(c))
	}
	stat := true
	for _, v := range c {
		if len(v.Values) != len(v.DSNames) {
			continue
		}
		if len(v.Values) != len(v.DSTypes) {
			continue
		}
		msgs := this.ParseJSON(v)
		if err := this.PersistData(msgs); err != nil {
			stat = false
			break
		}
	}
	m.ResponseChannel <- &nsq.FinishedMessage{m.Id, 0, stat}
}

func (this *MsgDeliver) PersistData(msgs []*Record) error {
	session := this.MSession.Copy()
	defer session.Close()
	var err error
	for _, msg := range msgs {
		var new_value float64
		if msg.DSType == "counter" || msg.DSType == "derive" {
			new_value, err = this.gen_new_value(msg)
		} else {
			new_value = msg.Value
		}
		if err != nil && err.Error() == "ignore" {
			continue
		}
		if err != nil {
			log.Println("fail to get new value", err)
			return err
		}
		op := &RedisOP{
			Action: "SADD",
			Key:    msg.Host,
			Value:  msg.Key,
			Done:   make(chan int),
		}
		this.RedisChan <- op
		<-op.Done
		n_v := &KeyValue{
			Timestamp: msg.Timestamp,
			Value:     new_value,
		}
		op = &RedisOP{
			Action: "ZADD",
			Key:    "archive:" + msg.Key,
			Value:  n_v,
			Done:   make(chan int),
		}
		this.RedisChan <- op
		<-op.Done
		if op.Err != nil {
			log.Println(op.Err)
			break
		}
		body := fmt.Sprintf("%d:%.2f", msg.Timestamp, msg.Value)
		op = &RedisOP{
			Action: "SET",
			Key:    "raw:" + msg.Key,
			Value:  body,
			Done:   make(chan int),
		}
		this.RedisChan <- op
		<-op.Done
		op = &RedisOP{
			Action: "SET",
			Key:    msg.Key,
			Value:  new_value,
			Done:   make(chan int),
		}
		this.RedisChan <- op
		<-op.Done
		if op.Err != nil {
			log.Println(op.Err)
			break
		}
	}
	return err
}

func (this *MsgDeliver) Redis() {
	redis_con := this.RedisPool.Get()
	for {
		op := <-this.RedisChan
		switch op.Action {
		case "GET":
			op.Result, op.Err = redis_con.Do(op.Action,
				op.Key)
		case "ZADD":
			v := op.Value.(*KeyValue)
			body := fmt.Sprintf("%d:%.2f", v.Timestamp, v.Value)
			op.Result, op.Err = redis_con.Do(op.Action,
				op.Key, v.Timestamp, body)
		case "ZREMRANGEBYSCORE":
			fallthrough
		case "ZRANGEBYSCORE":
			v := op.Value.([]interface{})
			if len(v) < 2 {
				op.Err = errors.New("wrong arg")
			} else {
				op.Result, op.Err = redis_con.Do(op.Action,
					op.Key, v[0], v[1])
			}
		default:
			op.Result, op.Err = redis_con.Do(op.Action,
				op.Key, op.Value)
		}
		if op.Err != nil {
			redis_con = this.RedisPool.Get()
		}
		op.Done <- 1
	}
}

func (this *MsgDeliver) gen_new_value(msg *Record) (float64, error) {
	var value float64
	op := &RedisOP{
		Action: "GET",
		Key:    "raw:" + msg.Key,
		Done:   make(chan int),
	}
	this.RedisChan <- op
	<-op.Done
	if op.Err != nil {
		return 0, op.Err
	}
	if op.Result == nil {
		return msg.Value, nil
	}
	body := string(op.Result.([]byte))
	kv := strings.Split(body, ":")
	var err error
	var tv KeyValue
	if len(kv) == 2 {
		tv.Timestamp, err = strconv.ParseInt(kv[0], 10, 64)
		tv.Value, err = strconv.ParseFloat(kv[1], 64)
		if err != nil {
			log.Println(kv, err)
		}
		if tv.Timestamp == msg.Timestamp {
			err = errors.New("ignore")
		}
		value = (msg.Value - tv.Value) /
			float64(msg.Timestamp-tv.Timestamp)
	} else {
		value = msg.Value
	}
	if value < 0 {
		value = 0
	}
	return value, nil
}
