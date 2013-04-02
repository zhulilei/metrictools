package main

import (
	metrictools "../"
	"encoding/json"
	"github.com/datastream/cal"
	"github.com/datastream/nsq/nsq"
	"github.com/garyburd/redigo/redis"
	"labix.org/v2/mgo/bson"
	"log"
	"strconv"
	"time"
)

type TriggerTask struct {
	exitChan chan int
	nw       *nsq.Writer
	*metrictools.MsgDeliver
	notifyTopic         string
	triggerCollection   string
	statisticCollection string
}

func trigger_task(msg_deliver *metrictools.MsgDeliver, w *nsq.Writer, topic string, tg string, st string) {
	for {
		m := <-msg_deliver.MessageChan
		t := &TriggerTask{
			exitChan:            make(chan int),
			nw:                  w,
			MsgDeliver:          msg_deliver,
			notifyTopic:         topic,
			triggerCollection:   tg,
			statisticCollection: st,
		}
		go t.trigger_task(m)
	}
}
func (this *TriggerTask) trigger_task(m *metrictools.Message) {
	var trigger metrictools.Trigger
	m.ResponseChannel <- &nsq.FinishedMessage{
		m.Id, 0, true}
	if err := json.Unmarshal(m.Body, &trigger); err != nil {
		return
	}
	go this.update_trigger(trigger.Expression)
	go this.calculate(trigger)
	go this.statistic(trigger)
}

func (this *TriggerTask) update_trigger(exp string) {
	session := this.MSession.Copy()
	defer session.Close()
	ticker := time.NewTicker(time.Second * 55)
	for {
		now := time.Now().Unix()
		err := session.DB(this.DBName).C(this.triggerCollection).
			Update(bson.M{"e": exp}, bson.M{"u": now})
		if err != nil {
			log.Println(err)
			close(this.exitChan)
			return
		}
		select {
		case <-this.exitChan:
			return
		case <-ticker.C:
		}
	}
}

// calculate trigger.exp
func (this *TriggerTask) calculate(trigger metrictools.Trigger) {
	session := this.MSession.Clone()
	defer session.Close()
	ticker := time.Tick(time.Minute * time.Duration(trigger.Interval))
	id := 0
	redis_con := this.RedisPool.Get()
	for {
		rst, err := calculate_exp(this.RedisQueryChan,
			trigger.Expression)
		if err != nil {
			close(this.exitChan)
			return
		}
		_, err = redis_con.Do("SET", "period_calculate_task:"+
			trigger.Expression+":"+strconv.Itoa(id), rst)
		if err != nil {
			close(this.exitChan)
			return
		}
		redis_con.Do("EXPIRE", "period_calculate_task:"+
			trigger.Expression+":"+
			strconv.Itoa(id), trigger.Period*60)
		id++
		if trigger.Insertable {
			record := metrictools.Record{
				Key:       trigger.Name,
				Value:     rst,
				Timestamp: time.Now().Unix(),
			}
			session.DB(this.DBName).C(this.statisticCollection).
				Insert(record)
		}
		select {
		case <-this.exitChan:
			return
		case <-ticker:
		}
	}
}

func calculate_exp(qchan chan metrictools.RedisQuery, exp string) (float64, error) {
	exp_list := cal.Parser(exp)
	k_v := make(map[string]interface{})
	var err error
	for _, item := range exp_list {
		if len(item) > 0 {
			q := metrictools.RedisQuery{
				Key:   item,
				Value: make(chan []byte),
			}
			qchan <- q
			v := <-q.Value
			var d float64
			if v == nil {
				d, err = strconv.ParseFloat(
					item, 64)
			} else {
				d, err = strconv.ParseFloat(
					string(v), 64)
			}
			if err == nil {
				k_v[item] = d
			} else {
				log.Println("failed to load value of",
					item)
			}
		}
	}
	rst, err := cal.Cal(exp, k_v)
	return rst, err
}

//get statistic result
func (this *TriggerTask) statistic(trigger metrictools.Trigger) {
	session := this.MSession.Clone()
	ticker := time.Tick(time.Minute * time.Duration(trigger.Period))
	redis_con := this.RedisPool.Get()
	for {
		key := "period_calculate_task:" + trigger.Expression + "*"
		values, err := get_redis_keys_values(redis_con, key)
		if err != nil {
			close(this.exitChan)
			return
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
					this.nw.Write(cmd)
				} else {
					log.Println("json nofity", err)
				}
			}
		}
		select {
		case <-this.exitChan:
			return
		case <-ticker:
		}
	}
}

// read keys and return keys' values
func get_redis_keys_values(redis_con redis.Conn, key string) ([]float64, error) {
	v, err := redis_con.Do("KEYS", key)
	if err != nil {
		return nil, err
	}
	keys, _ := v.([]interface{})
	var values []float64
	for i := range keys {
		key, _ := keys[i].([]byte)
		v, err := redis_con.Do("GET", string(key))
		if err == nil {
			if value, ok := v.([]byte); ok {
				d, e := strconv.ParseFloat(string(value), 64)
				if e == nil {
					values = append(values, d)
				} else {
					log.Println(string(value),
						" convert to float64 failed")
				}
			}
		} else {
			return nil, err
		}
	}
	return values, nil
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
