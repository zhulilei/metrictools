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
	dataservice   *redis.Pool
	configservice *redis.Pool
	writer        *nsq.Writer
	nsqd_address  string
	topic         string
}

func (this *TriggerTask) HandleMessage(m *nsq.Message) error {
	triggerChan := make(chan int)
	name := string(m.Body)
	config_con := this.configservice.Get()
	defer config_con.Close()
	interval, _ := redis.Int(config_con.Do("HGET", name, "interval"))
	period, _ := redis.Int(config_con.Do("HGET", name, "period"))
	persist, _ := redis.Bool(config_con.Do("HGET", name, "persist"))
	exp, _ := redis.String(config_con.Do("HGET", name, "exp"))
	relation, _ := redis.Int(config_con.Do("HGET", name, "relation"))
	w, _ := redis.Float64(config_con.Do("HGET", name, "warning"))
	e, _ := redis.Float64(config_con.Do("HGET", name, "error"))
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
	config_con := this.configservice.Get()
	defer config_con.Close()
	for {
		now := time.Now().Unix()
		_, err := config_con.Do("HSET", name, "last", now)
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
	data_con := this.dataservice.Get()
	defer data_con.Close()
	for {
		v, err := calculate_exp(this, trigger.Expression)
		if err != nil {
			close(triggerchan)
			return
		}

		go this.check_level(trigger, v)
		t := time.Now().Unix()
		if trigger.Persist {
			_, err = data_con.Do("ZADD", "archive:"+trigger.Name, t, v)
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

func (this *TriggerTask) check_level(trigger metrictools.Trigger, v float64) {
	newlevel := Judge_value(trigger, v)
	config_con := this.configservice.Get()
	defer config_con.Close()
	l, err := config_con.Do("HGET", "trigger:"+trigger.Name, "level")
	if err == nil {
		var level int
		if l != nil {
			level, _ = strconv.Atoi(string(l.([]byte)))
		} else {
			config_con.Do("HSET", "trigger:"+trigger.Name, "level", newlevel)
		}
		if level != newlevel {
			notify := &metrictools.Notify{
				Name:  trigger.Name,
				Level: newlevel,
				Value: v,
			}
			if body, err := json.Marshal(notify); err == nil {
				_, _, err := this.writer.Publish(this.topic, body)
				if err != nil {
					this.writer.ConnectToNSQ(this.nsqd_address)
				}
			} else {
				log.Println("json nofity", err)
			}
		}
	}
}
func calculate_exp(t *TriggerTask, exp string) (float64, error) {
	exp_list := cal.Parser(exp)
	k_v := make(map[string]interface{})
	config_con := t.configservice.Get()
	defer config_con.Close()
	for _, item := range exp_list {
		if len(item) > 0 {
			v, err := config_con.Do("GET", item)
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
