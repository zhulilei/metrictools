package main

import (
	metrictools "../"
	"encoding/json"
	"github.com/datastream/cal"
	"github.com/datastream/nsq/nsq"
	"labix.org/v2/mgo/bson"
	"log"
	"strconv"
	"time"
)

// add map to maintain current calculating exp
type TriggerTask struct {
	*metrictools.MsgDeliver
	writer              *nsq.Writer
	notifyTopic         string
	triggerCollection   string
	statisticCollection string
}

func (this *TriggerTask) HandleMessage(m *nsq.Message) error {
	var trigger metrictools.Trigger
	if err := json.Unmarshal(m.Body, &trigger); err == nil {
		triggerChan := make(chan int)
		go this.update_trigger(trigger.Expression, triggerChan)
		go this.calculate(trigger, triggerChan)
		go this.statistic(trigger, triggerChan)
	}
	return nil
}

func (this *TriggerTask) update_trigger(exp string, triggerchan chan int) {
	session := this.MSession.Copy()
	defer session.Close()
	ticker := time.NewTicker(time.Second * 55)
	for {
		now := time.Now().Unix()
		err := session.DB(this.DBName).C(this.triggerCollection).
			Update(bson.M{"e": exp}, bson.M{"u": now})
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
	session := this.MSession.Clone()
	defer session.Close()
	ticker := time.Tick(time.Minute * time.Duration(trigger.Interval))
	for {
		v, err := calculate_exp(this, trigger.Expression)
		if err != nil {
			close(triggerchan)
			return
		}
		t := time.Now().Unix()
		_, err = this.RedisDo("ZADD", "statistic:"+trigger.Name, []interface{}{t, v})
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
			v, err := t.RedisDo("GET", item, nil)
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
	session := this.MSession.Clone()
	ticker := time.Tick(time.Minute * time.Duration(trigger.Period))
	for {
		e := time.Now().Unix()
		s := e - int64(trigger.Interval)
		rst, err := this.RedisDo("ZRANGEBYSCORE",
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
		var tg metrictools.Trigger
		err = session.DB(this.DBName).C(this.triggerCollection).
			Find(bson.M{"e": trigger.Expression}).One(&tg)
		if err == nil {
			if tg.Stat != stat {
				notify := &metrictools.Notify{
					Name:  trigger.Name,
					Level: stat,
					Value: value,
				}
				if body, err := json.Marshal(notify); err == nil {
					cmd := nsq.Publish(this.notifyTopic,
						body)
					this.writer.Write(cmd)
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
