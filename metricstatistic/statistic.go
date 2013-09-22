package main

import (
	metrictools "../"
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
	info, err := redis.Values(config_con.Do("HMGET", trigger_name, "exp", "name"))
	if err != nil {
		log.Println("get trigger failed", trigger_name, err)
		return
	}
	var trigger metrictools.Trigger
	redis.Scan(info, &trigger.Expression, &trigger.Name)

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
