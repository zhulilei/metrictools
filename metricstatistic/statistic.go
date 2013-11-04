package main

import (
	metrictools "../"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/datastream/cal"
	"github.com/datastream/skyline"
	"github.com/garyburd/redigo/redis"
	"log"
	"strconv"
	"strings"
	"time"
)

// TriggerTask define a trigger statistic task
type TriggerTask struct {
	dataService  *redis.Pool
	writer       *nsq.Writer
	nsqdAddress  string
	topic        string
	FullDuration int64
	Consensus    int
}

// HandleMessage is TriggerTask's nsq handle function
func (m *TriggerTask) HandleMessage(msg *nsq.Message) error {
	name := string(msg.Body)
	dataCon := m.dataService.Get()
	defer dataCon.Close()
	now := time.Now().Unix()
	go dataCon.Do("HSET", name, "last", now)
	go m.calculate(name)
	return nil
}

func (m *TriggerTask) calculate(triggerName string) {
	dataCon := m.dataService.Get()
	defer dataCon.Close()
	var trigger metrictools.Trigger
	var err error
	trigger.IsExpression, err = redis.Bool(dataCon.Do("HGET", triggerName, "is_e"))
	if err != nil {
		log.Println("get trigger failed", triggerName, err)
		return
	}
	trigger.Name = triggerName
	if trigger.IsExpression {
		expList := cal.Parser(trigger.Name)
		v, err := m.calculateExp(expList)
		if err != nil {
			log.Println("calculate failed", trigger.Name, err)
			return
		}
		t := time.Now().Unix()
		body := fmt.Sprintf("%d:%.2f", t, v)
		_, err = dataCon.Do("ZADD", "archive:"+trigger.Name, t, body)
		_, err = dataCon.Do("ZREMRANGEBYSCORE", "archive:"+trigger.Name, 0, t-m.FullDuration-600)
	}
	go m.checkvalue(trigger.Name, trigger.IsExpression)
}

// ParseTimeSeries convert redis value to skyline's data format
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

func (m *TriggerTask) calculateExp(expList []string) (float64, error) {
	kv := make(map[string]interface{})
	dataCon := m.dataService.Get()
	defer dataCon.Close()
	exp := ""
	current := time.Now().Unix()
	for _, item := range expList {
		var v float64
		t := time.Now().Unix()
		values, err := redis.Values(dataCon.Do("HMGET", item, "rate_value", "timestamp"))
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
		kv[item] = v
		exp += item
	}
	rst, err := cal.Cal(exp, kv)
	return rst, err
}

func (m *TriggerTask) checkvalue(exp string, isExpression bool) {
	dataCon := m.dataService.Get()
	defer dataCon.Close()
	t := time.Now().Unix()
	values, err := redis.Strings(dataCon.Do("ZRANGEBYSCORE", "archive:"+exp, t-m.FullDuration-120, t))
	var skylineTrigger []string
	threshold := 8 - m.Consensus
	if err == nil {
		timeseries := ParseTimeSeries(values)
		if len(timeseries) == 0 {
			log.Println(exp, " is empty")
			return
		}
		timeSize := timeseries[len(timeseries)-1].Timestamp - timeseries[0].Timestamp
		if timeSize < m.FullDuration {
			log.Println("incomplete data", exp, timeSize)
			return
		}
		if skyline.MedianAbsoluteDeviation(timeseries) {
			skylineTrigger = append(skylineTrigger, "MedianAbsoluteDeviation")
		}
		if skyline.Grubbs(timeseries) {
			skylineTrigger = append(skylineTrigger, "Grubbs")
		}
		if skyline.FirstHourAverage(timeseries, m.FullDuration) {
			skylineTrigger = append(skylineTrigger, "FirstHourAverage")
		}
		if skyline.SimpleStddevFromMovingAverage(timeseries) {
			skylineTrigger = append(skylineTrigger, "SimpleStddevFromMovingAverage")
		}
		if skyline.StddevFromMovingAverage(timeseries) {
			skylineTrigger = append(skylineTrigger, "StddevFromMovingAverage")
		}
		if skyline.MeanSubtractionCumulation(timeseries) {
			skylineTrigger = append(skylineTrigger, "MeanSubtractionCumulation")
		}
		if skyline.LeastSquares(timeseries) {
			skylineTrigger = append(skylineTrigger, "LeastSquares")
		}
		if skyline.HistogramBins(timeseries) {
			skylineTrigger = append(skylineTrigger, "HistogramBins")
		}
		if (8 - len(skylineTrigger)) <= threshold {
			rst := make(map[string]string)
			rst["time"] = time.Now().Format("2006-01-02 15:04:05")
			rst["event"] = strings.Join(skylineTrigger, ", ")
			h := sha1.New()
			h.Write([]byte(exp))
			name := base64.URLEncoding.EncodeToString(h.Sum(nil))
			rst["trigger"] = name
			rst["trigger_exp"] = exp
			if isExpression {
				rst["url"] = "/api/v1/trigger/" + name
			} else {
				rst["url"] = "/api/v1/metric/" + exp
			}
			if body, err := json.Marshal(rst); err == nil {
				m.writer.Publish(m.topic, body)
				log.Println(string(body))
			}
		}
		if len(skylineTrigger) > 4 {
			log.Println(skylineTrigger, exp)
		}
	}
}
