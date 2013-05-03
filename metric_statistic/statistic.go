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
	dataservice   *metrictools.RedisService
	configservice *metrictools.RedisService
	writer        *nsq.Writer
	topic         string
}

func (this *TriggerTask) HandleMessage(m *nsq.Message) error {
	triggerChan := make(chan int)
	name := string(m.Body)
	interval, _ := redis.Int(this.configservice.Do("HGET", name, "interval"))
	period, _ := redis.Int(this.configservice.Do("HGET", name, "period"))
	persist, _ := redis.Bool(this.configservice.Do("HGET", name, "persist"))
	exp, _ := redis.String(this.configservice.Do("HGET", name, "exp"))
	relation, _ := redis.Int(this.configservice.Do("HGET", name, "relation"))
	w, _ := redis.Float64(this.configservice.Do("HGET", name, "warning"))
	e, _ := redis.Float64(this.configservice.Do("HGET", name, "error"))
	n := strings.Split(name, ":")
	trigger := metrictools.Trigger{
		Name:       n[1],
		Interval:   interval,
		Expression: exp,
		Period:     period,
		Persist:    persist,
		WValue:     w,
		EValue:     e,
		Relation:   relation,
	}
	go this.update_trigger(name, triggerChan)
	go this.calculate(trigger, triggerChan)
	return nil
}

func (this *TriggerTask) update_trigger(name string, triggerchan chan int) {
	ticker := time.NewTicker(time.Second * 55)
	for {
		now := time.Now().Unix()
		_, err := this.configservice.Do("HSET", name,
			[]interface{}{"last", now})
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

		go this.check_stat(trigger, v)
		t := time.Now().Unix()
		if trigger.Persist {
			_, err = this.dataservice.Do("ZADD",
				"archive:"+trigger.Name, []interface{}{t, v})
		}
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

func (this *TriggerTask) check_stat(trigger metrictools.Trigger, v float64) {
	newstate := Judge_value(trigger, v)
	s, err := this.configservice.Do("HGET", "trigger:"+trigger.Name, "stat")
	if err == nil {
		var state int
		if s != nil {
			state, _ = strconv.Atoi(string(s.([]byte)))
		} else {
			s, err = this.configservice.Do("HSET",
				"trigger:"+trigger.Name,
				[]interface{}{"stat", newstate})
		}
		if state != newstate {
			notify := &metrictools.Notify{
				Name:  trigger.Name,
				Level: newstate,
				Value: v,
			}
			if body, err := json.Marshal(notify); err == nil {
				_, _, _ = this.writer.Publish(this.topic, body)
			} else {
				log.Println("json nofity", err)
			}
		}
	}
}
func calculate_exp(t *TriggerTask, exp string) (float64, error) {
	exp_list := cal.Parser(exp)
	k_v := make(map[string]interface{})
	for _, item := range exp_list {
		if len(item) > 0 {
			v, err := t.configservice.Do("GET", item, nil)
			if err != nil {
				return 0, err
			}
			var d float64
			if v == nil {
				d, err = strconv.ParseFloat(item, 64)
			} else {
				d, err = strconv.ParseFloat(string(v.([]byte)), 64)
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

// return trigger's stat, 0 is ok, 1 is warning, 2 is error
func Judge_value(S metrictools.Trigger, value float64) int {
	switch S.Relation {
	case metrictools.LESS:
		{
			if value < S.EValue {
				return 2
			}
			if value < S.WValue {
				return 1
			}
		}
	case metrictools.GREATER:
		{
			if value > S.EValue {
				return 2
			}
			if value > S.WValue {
				return 1
			}
		}
	}
	return 0
}
