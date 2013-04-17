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
	"time"
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
		n_v := &KeyValue{
			Timestamp: msg.Timestamp,
			Value:     new_value,
		}
		_, err = this.RedisDo("ZADD", "archive:"+msg.Key, n_v)
		if err != nil {
			log.Println(err)
			break
		}
		body := fmt.Sprintf("%d:%.2f", msg.Timestamp, msg.Value)
		_, err = this.RedisDo("SET", "raw:"+msg.Key, body)
		if err != nil {
			log.Println("set raw", err)
			break
		}
		_, err = this.RedisDo("SET", msg.Key, new_value)
		if err != nil {
			log.Println("last data", err)
			break
		}
	}
	return err
}

func (this *MsgDeliver) CompressData() {
	tick := time.Tick(time.Minute * 10)
	for {
		rst, err := this.RedisDo("KEYS", "archive:*", nil)
		if err == nil {
			value_list := rst.([]interface{})
			for _, value := range value_list {
				this.remove_old(value.([]byte))
				this.remove_dup(value.([]byte))
			}
		}
		<-tick
	}
}

func (this *MsgDeliver) remove_old(key []byte) {
	lastweek := time.Now().Unix() - 7*24*3600
	_, err := this.RedisDo("ZREMRANGEBYSCORE",
		string(key), []interface{}{0, lastweek})
	if err != nil {
		log.Println("last data", err)
	}
}

func (this *MsgDeliver) remove_dup(key []byte) {
	index := int64(0)
	count := this.GetSetSize(string(key))
	for {
		if index > count {
			break
		}
		rst, err := this.RedisDo("ZRANGE", string(key),
			[]interface{}{index, index + 5})
		if err == nil {
			value_list := rst.([]interface{})
			var last_key interface{}
			var last_v float64
			var last_t int64
			last_v = -1
			for _, value := range value_list {
				t, v, _ := GetTimestampValue(string(value.([]byte)))
				if v != last_v {
					last_v = v
					last_t = t
					last_key = value
					continue
				}
				if (t - last_t) > 3600 {
					last_t = t
					continue
				}
				this.RedisDo("ZREM",
					string(key), last_key)
				last_key = value
				last_t = t
				count--
				index--
			}
		}
		index = index + 5
	}
}

func (this *MsgDeliver) GetSetSize(key string) int64 {
	rst, err := this.RedisDo("ZCARD", key, nil)
	var count int64
	if err == nil {
		v, ok := rst.(int64)
		if ok {
			count = v
		} else {
			log.Println("ZCARD return not int64")
		}
	}
	return count
}

func (this *MsgDeliver) RedisDo(action string, key string, value interface{}) (interface{}, error) {
	op := &RedisOP{
		Action: action,
		Key:    key,
		Value:  value,
		Done:   make(chan int),
	}
	this.RedisChan <- op
	<-op.Done
	return op.Result, op.Err
}

func GetTimestamp(key string) (int64, error) {
	t, _, err := GetTimestampValue(key)
	return t, err
}

func GetValue(key string) (float64, error) {
	_, v, err := GetTimestampValue(key)
	return v, err
}

func GetTimestampValue(key string) (int64, float64, error) {
	body := string(key)
	kv := strings.Split(body, ":")
	var t int64
	var v float64
	var err error
	if len(kv) == 2 {
		t, err = strconv.ParseInt(kv[0], 10, 64)
		v, err = strconv.ParseFloat(kv[1], 64)
		if err != nil {
			log.Println(kv, err)
		}
	} else {
		err = errors.New("wrong data")
	}
	return t, v, err
}

func (this *MsgDeliver) Redis() {
	redis_con := this.RedisPool.Get()
	for {
		op := <-this.RedisChan
		switch op.Action {
		case "ZCARD":
			fallthrough
		case "KEYS":
			fallthrough
		case "GET":
			op.Result, op.Err = redis_con.Do(op.Action,
				op.Key)
		case "ZREM":
			op.Result, op.Err = redis_con.Do(op.Action,
				op.Key, op.Value)
		case "ZADD":
			v := op.Value.(*KeyValue)
			body := fmt.Sprintf("%d:%.2f", v.Timestamp, v.Value)
			op.Result, op.Err = redis_con.Do(op.Action,
				op.Key, v.Timestamp, body)
		case "ZRANGE":
			fallthrough
		case "ZRANGEBYSCORE":
			fallthrough
		case "ZREMRANGEBYSCORE":
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
	rst, err := this.RedisDo("GET", "raw:"+msg.Key, nil)
	if err != nil {
		return 0, err
	}
	if rst == nil {
		return msg.Value, nil
	}
	var value float64
	t, v, err := GetTimestampValue(string(rst.([]byte)))
	if err == nil {
		value = (msg.Value - v) /
			float64(msg.Timestamp-t)
	} else {
		value = msg.Value
	}
	if value < 0 {
		value = 0
	}
	return value, nil
}
