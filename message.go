package metrictools

import (
	"encoding/json"
	"errors"
	"github.com/datastream/nsq/nsq"
	"github.com/garyburd/redigo/redis"
	"labix.org/v2/mgo"
	"log"
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
	action string
	key    string
	value  interface{}
	result interface{}
	err    error
	done   chan int
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

func (this *MsgDeliver) ProcessData(collection string) {
	for {
		m := <-this.MessageChan
		go this.insert_data(collection, m)
	}
}

func (this *MsgDeliver) insert_data(collection string, m *Message) {
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
		if err := this.PersistData(msgs, collection); err != nil {
			stat = false
			break
		}
	}
	m.ResponseChannel <- &nsq.FinishedMessage{m.Id, 0, stat}
}

func (this *MsgDeliver) PersistData(msgs []*Record, collection string) error {
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
			action: "SADD",
			key:    msg.Host,
			value:  msg.Key,
			done:   make(chan int),
		}
		this.RedisChan <- op
		<-op.done
		n_v := &KeyValue{
			Timestamp: msg.Timestamp,
			Value:     new_value,
		}
		op = &RedisOP{
			action: "ZADD",
			key:    "archive:" + msg.Key,
			value:  n_v,
			done:   make(chan int),
		}
		this.RedisChan <- op
		<-op.done
		if op.err != nil {
			log.Println(op.err)
			break
		}
		v := &KeyValue{
			Timestamp: msg.Timestamp,
			Value:     msg.Value,
		}
		var body []byte
		if body, err = json.Marshal(v); err == nil {
			op = &RedisOP{
				action: "SET",
				key:    "raw_" + msg.Key,
				value:  body,
				done:   make(chan int),
			}
			this.RedisChan <- op
			<-op.done
			if op.err != nil {
				log.Println(op.err)
				break
			}
		}
	}
	return err
}

func (this *MsgDeliver) Redis() {
	redis_con := this.RedisPool.Get()
	for {
		op := <-this.RedisChan
		switch op.action {
		case "GET":
			op.result, op.err = redis_con.Do(op.action,
				op.key)
		case "ZADD":
			v := op.value.(*KeyValue)
			if body, err := json.Marshal(v); err == nil {
				op.result, op.err = redis_con.Do(op.action,
					op.key, v.Timestamp, body)
			}
		default:
			op.result, op.err = redis_con.Do(op.action,
				op.key, op.value)
		}
		if op.err != nil {
			redis_con = this.RedisPool.Get()
		}
		op.done <- 1
	}
}

func (this *MsgDeliver) gen_new_value(msg *Record) (float64, error) {
	var value float64
	op := &RedisOP{
		action: "GET",
		key:    "raw_" + msg.Key,
		done:   make(chan int),
	}
	this.RedisChan <- op
	<-op.done
	if op.err != nil {
		return 0, op.err
	}
	if op.result == nil {
		return msg.Value, nil
	}
	var tv KeyValue
	if err := json.Unmarshal(op.result.([]byte), &tv); err == nil {
		if tv.Timestamp == msg.Timestamp {
			err = errors.New("ignore")
		}
		if tv.Timestamp < msg.Timestamp {
			value = (msg.Value - tv.Value) /
				float64(msg.Timestamp-tv.Timestamp)
		}
		if tv.Timestamp > msg.Timestamp {
			value = (tv.Value - msg.Value) /
				float64(tv.Timestamp-msg.Timestamp)
		}
		if value < 0 {
			value = 0
		}
	} else {
		log.Println(msg.Value, "raw data", err)
		value = msg.Value
	}
	return value, nil
}
