package metrictools

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bitly/nsq/nsq"
	"github.com/garyburd/redigo/redis"
	"log"
	"strconv"
	"strings"
	"time"
)

type MsgDeliver struct {
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

func (this *MsgDeliver) HandleMessage(m *nsq.Message) error {
	var err error
	var c []CollectdJSON
	if err = json.Unmarshal(m.Body, &c); err != nil {
		log.Println(err)
		return nil
	}
	if this.VerboseLogging {
		log.Println("RAW JSON String: ", string(m.Body))
		log.Println("JSON SIZE: ", len(c))
	}
	for _, v := range c {
		if len(v.Values) != len(v.DSNames) {
			continue
		}
		if len(v.Values) != len(v.DSTypes) {
			continue
		}
		msgs := v.ToRecord()
		if err := this.PersistData(msgs); err != nil {
			return err
		}
	}
	return nil
}

func (this *MsgDeliver) PersistData(msgs []*Record) error {
	var err error
	for _, msg := range msgs {
		var new_value float64
		if msg.DSType == "counter" || msg.DSType == "derive" {
			new_value, err = this.getRate(msg)
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
		_, err = RedisDo(this.RedisChan, "ZADD", "archive:"+msg.Key,
			[]interface{}{msg.Timestamp, new_value})
		if err != nil {
			log.Println(err)
			break
		}
		body := fmt.Sprintf("%d:%.2f", msg.Timestamp, msg.Value)
		_, err = RedisDo(this.RedisChan, "SET", "raw:"+msg.Key, body)
		if err != nil {
			log.Println("set raw", err)
			break
		}
		_, err = RedisDo(this.RedisChan, "SET", msg.Key, new_value)
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
		rst, err := RedisDo(this.RedisChan, "KEYS", "archive:*", nil)
		if err == nil {
			value_list := rst.([]interface{})
			for _, value := range value_list {
				this.remove_old(value.([]byte))
				this.do_compress(value.([]byte))
			}
		}
		<-tick
	}
}

func (this *MsgDeliver) remove_old(key []byte) {
	lastweek := time.Now().Unix() - 60*24*3600
	_, err := RedisDo(this.RedisChan, "ZREMRANGEBYSCORE",
		string(key), []interface{}{0, lastweek})
	if err != nil {
		log.Println("last data", err)
	}
}

func (this *MsgDeliver) do_compress(key []byte) {
	current := time.Now().Unix() - 60*24*3600
	last_d := time.Now().Unix() - 24*3600
	last_2d := time.Now().Unix() - 2*24*3600
	var interval int64
	for {
		if current > last_d {
			break
		}
		if current < last_2d {
			interval = 600
		} else {
			interval = 300
		}
		rst, err := RedisDo(this.RedisChan, "ZRANGEBYSCORE", string(key),
			[]interface{}{current, current + interval})
		if err == nil {
			value_list := rst.([]interface{})
			sumvalue := float64(0)
			sumtime := int64(0)
			for _, value := range value_list {
				t, v, _ := GetTimestampValue(string(value.([]byte)))
				sumvalue += v
				sumtime += t
				RedisDo(this.RedisChan, "ZREM", string(key), value)
			}
			size := len(value_list)
			if size > 0 {
				RedisDo(this.RedisChan, "ZADD", string(key),
					[]interface{}{sumtime / int64(size), sumvalue / float64(size)})
			}
		}
	}
}

func (this *MsgDeliver) GetSetSize(key string) int64 {
	rst, err := RedisDo(this.RedisChan, "ZCARD", key, nil)
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

func RedisDo(redisChan chan *RedisOP, action string, key string, value interface{}) (interface{}, error) {
	op := &RedisOP{
		Action: action,
		Key:    key,
		Value:  value,
		Done:   make(chan int),
	}
	redisChan <- op
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

func Redis(redis_pool *redis.Pool, redisChan chan *RedisOP) {
	redis_con := redis_pool.Get()
	for {
		op := <-redisChan
		switch op.Action {
		case "ZCARD":
			fallthrough
		case "KEYS":
			fallthrough
		case "HMGETALL":
			fallthrough
		case "GET":
			op.Result, op.Err = redis_con.Do(op.Action,
				op.Key)
		case "HGET":
			fallthrough
		case "ZREM":
			op.Result, op.Err = redis_con.Do(op.Action,
				op.Key, op.Value)
		case "ZADD":
			v := op.Value.([]interface{})
			if len(v) != 2 {
				op.Err = errors.New("wrong args")
				op.Done <- 1
				continue
			}
			body := fmt.Sprintf("%d:%.2f", v[0], v[1])
			op.Result, op.Err = redis_con.Do(op.Action,
				op.Key, v[0], body)
		case "HSET":
			fallthrough
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
			redis_con = redis_pool.Get()
		}
		op.Done <- 1
	}
}

func (this *MsgDeliver) getRate(msg *Record) (float64, error) {
	rst, err := RedisDo(this.RedisChan, "GET", "raw:"+msg.Key, nil)
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
