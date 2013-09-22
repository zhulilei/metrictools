package main

import (
	metrictools "../"
	"errors"
	"fmt"
	nsq "github.com/bitly/go-nsq"
	"github.com/datastream/cal"
	"github.com/datastream/skyline"
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
	config_con := this.configservice.Get()
	defer config_con.Close()
	var trigger metrictools.Trigger
	var err error
	trigger.Expression, err = redis.String(config_con.Do("HGET", trigger_name, "exp"))
	if err != nil {
		log.Println("get trigger failed", trigger_name, err)
		return
	}
	trigger.Name = trigger_name[8:]
	v, err := calculate_exp(this, trigger.Expression)
	if err != nil {
		log.Println("calculate failed", trigger_name, err)
		return
	}

	t := time.Now().Unix()
	body := fmt.Sprintf("%d:%.2f", t, v)
	_, err = data_con.Do("ZADD", "archive:"+trigger.Name, t, body)
	_, err = data_con.Do("ZREMRANGEBYSCORE", "archive:"+trigger.Name, 0, t-3600)
	values, err := redis.Strings(data_con.Do("ZRANGEBYSCORE", "archive:"+trigger.Name, t-3600*3, t))
	if err != nil {
		timeseries := ParseTimeSeries(values)
		if skyline.MedianAbsoluteDeviation(timeseries) {
			log.Println("medianabsolutedeviation:", trigger.Name)
		}
		if skyline.Grubbs(timeseries) {
			log.Println("grubbs:", trigger.Name)
		}
		var one_hour []skyline.TimePoint
		l := len(timeseries)
		if l > 60 {
			one_hour = timeseries[l-60 : l]
		}
		if skyline.FirstHourAverage(one_hour, 0) {
			log.Println("firsthouraverage:", trigger.Name)
		}
		if skyline.SimpleStddevFromMovingAverage(timeseries) {
			log.Println("simplestddevfrommovingaverage:", trigger.Name)
		}
		if skyline.StddevFromMovingAverage(timeseries) {
			log.Println("stddevfrommovingaverage:", trigger.Name)
		}
		if skyline.MeanSubtractionCumulation(timeseries) {
			log.Println("meansubtractioncumulation:", trigger.Name)
		}
		if skyline.LeastSquares(timeseries) {
			log.Println("leastsquares:", trigger.Name)
		}
		if skyline.HistogramBins(timeseries) {
			log.Println("histogram:", trigger.Name)
		}
	}
}

func ParseTimeSeries(values []string) []skyline.TimePoint {
	var rst []skyline.TimePoint
	for _, val := range values {
		t, v, err := metrictools.GetTimestampAndValue(val)
		if err != nil {
			continue
		}
		timepoint := skyline.TimePoint{
			Timestamp: t,
			Value:     v,
		}
		rst = append(rst, timepoint)
	}
	return rst
}

func calculate_exp(t *TriggerTask, exp string) (float64, error) {
	exp_list := cal.Parser(exp)
	if len(exp_list) < 1 {
		return 0, errors.New("no exp")
	}
	var timeseries []int64
	k_v := make(map[string]interface{})
	data_con := t.dataservice.Get()
	defer data_con.Close()
	for _, item := range exp_list {
		if len(item) > 0 {
			var v float64
			var t int64
			values, err := redis.Values(data_con.Do("HMGET", item, "rate_value", "timestamp"))
			if err == nil {
				_, err = redis.Scan(values, &v, &t)
			}
			if err == redis.ErrNil {
				t = time.Now().Unix()
				v, err = strconv.ParseFloat(item, 64)
			}
			if err != nil {
				return 0, err
			}
			k_v[item] = v
			timeseries = append(timeseries, t)
		}
	}
	if len(exp_list) == 1 {
		return k_v[exp].(float64), nil
	}
	if !checktime(timeseries) {
		return 0, errors.New("some data are too old")
	}
	rst, err := cal.Cal(exp, k_v)
	return rst, err
}

func checktime(timeseries []int64) bool {
	max := timeseries[0]
	min := timeseries[0]
	for _, v := range timeseries {
		if max < v {
			max = v
		}
		if min > v {
			min = v
		}
	}
	if (max - min) > 90 {
		return false
	}
	return true
}
