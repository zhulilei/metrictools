package main

import (
	metrictools "../"
	"encoding/json"
	"github.com/bitly/nsq/nsq"
	"github.com/datastream/cal"
	"github.com/garyburd/redigo/redis"
	"log"
	"strconv"
	"strings"
	"time"
)

// add map to maintain current calculating exp
type TriggerTask struct {
	*metrictools.MsgDeliver
	writer              *nsq.Writer
	notifyTopic         string
	triggerCollection   string
	statisticCollection string
	ConfigRedisPool     *redis.Pool
	ConfigChan          chan *metrictools.RedisOP
}

func (this *TriggerTask) HandleMessage(m *nsq.Message) error {
	triggerChan := make(chan int)
	name := string(m.Body)
	interval, _ := redis.Int(metrictools.RedisDo(this.ConfigChan, "HGET", name, "interval"))
	period, _ := redis.Int(metrictools.RedisDo(this.ConfigChan, "HGET", name, "period"))
	ttype, _ := redis.Int(metrictools.RedisDo(this.ConfigChan, "HGET", name, "type"))
	exp, _ := redis.String(metrictools.RedisDo(this.ConfigChan, "HGET", name, "exp"))
	relation, _ := redis.Int(metrictools.RedisDo(this.ConfigChan, "HGET", name, "relation"))
	n := strings.Split(name, ":")
	trigger := metrictools.Trigger{
		Name:        n[1],
		Interval:    interval,
		Expression:  exp,
		Period:      period,
		TriggerType: ttype,
		Relation:    relation,
	}
	go this.update_trigger(name, triggerChan)
	go this.calculate(trigger, triggerChan)
	go this.statistic(trigger, triggerChan)
	return nil
}

func (this *TriggerTask) update_trigger(name string, triggerchan chan int) {
	ticker := time.NewTicker(time.Second * 55)
	for {
		now := time.Now().Unix()
		_, err := metrictools.RedisDo(this.ConfigChan, "HSET",
			name, []interface{}{"lastmodify", now})
		if err != nil {
			log.Println(err)
			close(triggerchan)
			return
		}
		select {
		case <-triggerchan:
			return
		case <-ticker.C:
		}
	}
}

// calculate trigger.exp
func (this *TriggerTask) calculate(trigger metrictools.Trigger, triggerchan chan int) {
	ticker := time.Tick(time.Minute * time.Duration(trigger.Interval))
	for {
		v, err := calculate_exp(this, trigger.Expression)
		if err != nil {
			close(triggerchan)
			return
		}
		t := time.Now().Unix()
		_, err = metrictools.RedisDo(this.RedisChan, "ZADD", "statistic:"+trigger.Name, []interface{}{t, v})
		if err != nil {
			close(triggerchan)
			return
		}
		select {
		case <-triggerchan:
			return
		case <-ticker:
		}
	}
}

func calculate_exp(t *TriggerTask, exp string) (float64, error) {
	exp_list := cal.Parser(exp)
	k_v := make(map[string]interface{})
	for _, item := range exp_list {
		if len(item) > 0 {
			v, err := metrictools.RedisDo(t.ConfigChan, "GET", item, nil)
			if err != nil {
				return 0, err
			}
			var d float64
			if v == nil {
				d, err = strconv.ParseFloat(item, 64)
			} else {
				d, err = strconv.ParseFloat(
					string(v.([]byte)), 64)
			}
			if err == nil {
				k_v[item] = d
			} else {
				log.Println("failed to load value of", item)
			}
		}
	}
	rst, err := cal.Cal(exp, k_v)
	return rst, err
}

//get statistic result
func (this *TriggerTask) statistic(trigger metrictools.Trigger, triggerchan chan int) {
	ticker := time.Tick(time.Minute * time.Duration(trigger.Period))
	for {
		e := time.Now().Unix()
		s := e - int64(trigger.Interval)
		rst, err := metrictools.RedisDo(this.RedisChan, "ZRANGEBYSCORE",
			"statistic:"+trigger.Name, []interface{}{s, e})
		if err != nil {
			close(triggerchan)
			return
		}
		md, ok := rst.([]interface{})
		if !ok {
			log.Println("not []interface{}")
			continue
		}
		var values []float64
		for _, v := range md {
			value, err := metrictools.GetValue(string(v.([]byte)))
			if err == nil {
				values = append(values, value)
			}
		}
		stat, value := check_value(trigger, values)

		v, err := metrictools.RedisDo(this.ConfigChan, "HGET",
			"trigger:"+trigger.Name, "stat")
		if err == nil {
			var state int
			if v != nil {
				state, _ = strconv.Atoi(string(v.([]byte)))
			} else {
				v, err = metrictools.RedisDo(this.ConfigChan,
					"HSET", "trigger:"+trigger.Name,
					[]interface{}{"stat", stat})
			}
			if stat != state {
				notify := &metrictools.Notify{
					Name:  trigger.Name,
					Level: stat,
					Value: value,
				}
				if body, err := json.Marshal(notify); err == nil {
					_, _, _ = this.writer.Publish(this.notifyTopic, body)
				} else {
					log.Println("json nofity", err)
				}
			}
		}
		select {
		case <-triggerchan:
			return
		case <-ticker:
		}
	}
}

// check trigger's statistic value
func check_value(trigger metrictools.Trigger, data []float64) (int, float64) {
	var rst float64
	switch trigger.TriggerType {
	case metrictools.AVG:
		{
			rst = Avg_value(data)
			return Judge_value(trigger, rst), rst
		}
	case metrictools.SUM:
		{
			rst = Sum_value(data)
			return Judge_value(trigger, rst), rst
		}
	case metrictools.MAX:
		{
			rst = Max_value(data)
			return Judge_value(trigger, rst), rst
		}
	case metrictools.MIN:
		{
			rst = Min_value(data)
			return Judge_value(trigger, rst), rst
		}
	}
	return 0, rst
}

// get average value for []float64
func Avg_value(r []float64) float64 {
	return Sum_value(r) / float64(len(r))
}

// get sum of []float64
func Sum_value(r []float64) float64 {
	var rst float64
	rst = r[0]
	for i := range r {
		rst += r[i]
	}
	return rst
}

// get max value in []float64
func Max_value(r []float64) float64 {
	var rst float64
	rst = r[0]
	for i := range r {
		if rst < r[i] {
			rst = r[i]
		}
	}
	return rst
}

// get min value in []float64
func Min_value(r []float64) float64 {
	var rst float64
	rst = r[0]
	for i := range r {
		if rst > r[i] {
			rst = r[i]
		}
	}
	return rst
}

// return trigger's stat, 0 is ok, 1 is warning, 2 is error
func Judge_value(S metrictools.Trigger, value float64) int {
	if len(S.Values) != 2 {
		return 0
	}
	switch S.Relation {
	case metrictools.LESS:
		{
			if value < S.Values[1] {
				return 2
			}
			if value < S.Values[0] {
				return 1
			}
		}
	case metrictools.GREATER:
		{
			if value > S.Values[1] {
				return 2
			}
			if value > S.Values[0] {
				return 1
			}
		}
	}
	return 0
}
