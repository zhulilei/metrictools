package main

import (
	metrictools "../"
	"fmt"
	"github.com/bitly/nsq/nsq"
	"github.com/datastream/cal"
	"github.com/garyburd/redigo/redis"
	"log"
	"strconv"
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
	t_name := string(m.Body)
	config_con := this.configservice.Get()
	defer config_con.Close()
	now := time.Now().Unix()
	go config_con.Do("HSET", t_name, "last", now)
	go this.calculate(t_name)
	return nil
}

// calculate trigger.exp
func (this *TriggerTask) calculate(trigger_name string) {
	data_con := this.dataservice.Get()
	defer data_con.Close()
	info, _ := redis.Values(data_con.Do("HGET", trigger_name, "interval", "period", "exp", "relation", "name"))
	var trigger metrictools.Trigger
	_, err := redis.Scan(info, &trigger.Interval, &trigger.Period, &trigger.Expression, &trigger.Relation, &trigger.Name)

	v, err := calculate_exp(this, trigger.Expression)
	if err != nil {
		return
	}

	t := time.Now().Unix()
	body := fmt.Sprintf("%d:%.2f", t, v)
	_, err = data_con.Do("ZADD", "archive:"+trigger.Name, t, body)
	_, err = data_con.Do("ZREMRANGEBYSCORE", "archive:"+trigger.Name, 0, t-3600)
}

func calculate_exp(t *TriggerTask, exp string) (float64, error) {
	exp_list := cal.Parser(exp)
	k_v := make(map[string]interface{})
	data_con := t.dataservice.Get()
	defer data_con.Close()
	for _, item := range exp_list {
		if len(item) > 0 {
			v, err := redis.Float64(data_con.Do("HGET", item, "rate_value"))
			if err == redis.ErrNil {
				v, err = strconv.ParseFloat(item, 64)
			}
			if err != nil {
				return 0, err
			}
			if err == nil {
				k_v[item] = v
			} else {
				log.Println("failed to load value of", item)
			}
		}
	}
	rst, err := cal.Cal(exp, k_v)
	return rst, err
}
