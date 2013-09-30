package main

import (
	metrictools "../"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	nsq "github.com/bitly/go-nsq"
	"github.com/datastream/cal"
	"github.com/datastream/skyline"
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
	exp_list := cal.Parser(trigger.Expression)
	if len(exp_list) < 1 {
		return
	}
	if len(exp_list) == 1 {
		go this.checkvalue(trigger.Expression, trigger.Expression)
	} else {
		v, err := this.calculate_exp(exp_list)
		if err != nil {
			log.Println("calculate failed", trigger_name, err)
			return
		}
		t := time.Now().Unix()
		body := fmt.Sprintf("%d:%.2f", t, v)
		_, err = data_con.Do("ZADD", "archive:"+trigger.Name, t, body)
		_, err = data_con.Do("ZREMRANGEBYSCORE", "archive:"+trigger.Name, 0, t-3600*24)
		go this.checkvalue(trigger.Name, trigger.Expression)
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

func (this *TriggerTask) calculate_exp(exp_list []string) (float64, error) {
	k_v := make(map[string]interface{})
	data_con := this.dataservice.Get()
	defer data_con.Close()
	exp := ""
	current := time.Now().Unix()
	for _, item := range exp_list {
		var v float64
		t := time.Now().Unix()
		values, err := redis.Values(data_con.Do("HMGET", item, "rate_value", "timestamp"))
		if err == nil {
			_, err = redis.Scan(values, &v, &t)
			if err != nil {
				return 0, err
			}
		}
		if err == redis.ErrNil {
			v, err = strconv.ParseFloat(item, 64)
		}
		if (current - t) > 60 {
			return 0, errors.New(item + "'s data are too old")
		}
		if err != nil {
			return 0, err
		}
		k_v[item] = v
		exp += item
	}
	rst, err := cal.Cal(exp, k_v)
	return rst, err
}

func (this *TriggerTask) checkvalue(archive, exp string) {
	data_con := this.dataservice.Get()
	defer data_con.Close()
	t := time.Now().Unix()
	values, err := redis.Strings(data_con.Do("ZRANGEBYSCORE", "archive:"+archive, t-3600*24, t))
	var skyline_trigger []string
	if err == nil {
		timeseries := ParseTimeSeries(values)
		if skyline.MedianAbsoluteDeviation(timeseries) {
			skyline_trigger = append(skyline_trigger, "MedianAbsoluteDeviation")
		}
		if skyline.Grubbs(timeseries) {
			skyline_trigger = append(skyline_trigger, "Grubbs")
		}
		l := len(timeseries)
		if l > 60 {
			one_hour := timeseries[l-60 : l]
			if skyline.FirstHourAverage(one_hour, 0) {
				skyline_trigger = append(skyline_trigger, "FirstHourAverage")
			}
		}
		if skyline.SimpleStddevFromMovingAverage(timeseries) {
			skyline_trigger = append(skyline_trigger, "SimpleStddevFromMovingAverage")
		}
		if skyline.StddevFromMovingAverage(timeseries) {
			skyline_trigger = append(skyline_trigger, "StddevFromMovingAverage")
		}
		if skyline.MeanSubtractionCumulation(timeseries) {
			skyline_trigger = append(skyline_trigger, "MeanSubtractionCumulation")
		}
		if skyline.LeastSquares(timeseries) {
			skyline_trigger = append(skyline_trigger, "LeastSquares")
		}
		if skyline.HistogramBins(timeseries) {
			skyline_trigger = append(skyline_trigger, "HistogramBins")
		}
		if len(skyline_trigger) > 0 {
			rst := make(map[string]string)
			rst["time"] = time.Now().Format("2006-01-02 15:04:05")
			rst["event"] = strings.Join(skyline_trigger, ", ")
			h := sha1.New()
			h.Write([]byte(exp))
			name := base64.URLEncoding.EncodeToString(h.Sum(nil))
			rst["trigger"] = name
			rst["trigger_exp"] =  exp
			if archive == exp {
				rst["url"] = "/api/v1/metric/" + archive
			} else {
				rst["url"] = "/api/v1/trigger/" + name
			}
			if body, err := json.Marshal(rst); err == nil {
				this.writer.Publish(this.topic, body)
				log.Println(string(body))
			}
		}
	}
}
