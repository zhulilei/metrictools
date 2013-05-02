package metrictools

import (
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"log"
	"strconv"
	"strings"
)

type RedisService struct {
	RedisChan chan *RedisOP
	RedisPool *redis.Pool
}

type RedisOP struct {
	Action string
	Key    string
	Value  interface{}
	Result interface{}
	Err    error
	Done   chan int
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

func (this *RedisService) GetSetSize(key string) int64 {
	rst, err := this.Do("ZCARD", key, nil)
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

func (this *RedisService) Run() {
	redis_con := this.RedisPool.Get()
	for {
		op := <-this.RedisChan
		switch op.Action {
		case "ZCARD":
			fallthrough
		case "KEYS":
			fallthrough
		case "HMGETALL":
			fallthrough
		case "SMEMBERS":
			fallthrough
		case "GET":
			op.Result, op.Err = redis_con.Do(op.Action,
				op.Key)
		case "HGET":
			fallthrough
		case "SREM":
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
			redis_con = this.RedisPool.Get()
		}
		op.Done <- 1
	}
}

func (this *RedisService) Do(action string, key string, value interface{}) (interface{}, error) {
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
