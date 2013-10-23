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
	dataService   *redis.Pool
	configService *redis.Pool
	writer        *nsq.Writer
	nsqdAddress   string
	topic         string
	FullDuration  int64
	Consensus     int
}

// HandleMessage is TriggerTask's nsq handle function
func (m *TriggerTask) HandleMessage(msg *nsq.Message) error {
	name := string(msg.Body)
	configCon := m.configService.Get()
	defer configCon.Close()
	now := time.Now().Unix()
	go configCon.Do("HSET", name, "last", now)
	go m.calculate(name)
	return nil
}

func (m *TriggerTask) calculate(triggerName string) {
	dataCon := m.dataService.Get()
	defer dataCon.Close()
	configCon := m.configService.Get()
	defer configCon.Close()
	var trigger metrictools.Trigger
	var err error
	trigger.Expression, err = redis.String(configCon.Do("HGET", triggerName, "exp"))
	if err != nil {
		log.Println("get trigger failed", triggerName, err)
		return
	}
	trigger.Name = triggerName[8:]
	expList := cal.Parser(trigger.Expression)
	if len(expList) < 1 {
		return
	}
	if len(expList) == 1 {
		go m.checkvalue(trigger.Expression, trigger.Expression)
	} else {
		v, err := m.calculateExp(expList)
		if err != nil {
			log.Println("calculate failed", triggerName, err)
			return
		}
		t := time.Now().Unix()
		body := fmt.Sprintf("%d:%.2f", t, v)
		_, err = dataCon.Do("ZADD", "archive:"+trigger.Name, t, body)
		_, err = dataCon.Do("ZREMRANGEBYSCORE", "archive:"+trigger.Name, 0, t-m.FullDuration-100)
		go m.checkvalue(trigger.Name, trigger.Expression)
	}
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

func (m *TriggerTask) checkvalue(archive, exp string) {
	dataCon := m.dataService.Get()
	defer dataCon.Close()
	t := time.Now().Unix()
	values, err := redis.Strings(dataCon.Do("ZRANGEBYSCORE", "archive:"+archive, t-m.FullDuration, t))
	var skylineTrigger []string
	threshold := 8 - m.Consensus
	if err == nil {
		timeseries := ParseTimeSeries(values)
		if len(timeseries) == 0 {
			log.Println(archive, " is empty")
			return
		}
		if (timeseries[len(timeseries)-1].Timestamp - timeseries[0].Timestamp) < m.FullDuration {
			log.Println("incomplete data", exp, archive)
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
			if archive == exp {
				rst["url"] = "/api/v1/metric/" + archive
			} else {
				rst["url"] = "/api/v1/trigger/" + name
			}
			if body, err := json.Marshal(rst); err == nil {
				m.writer.Publish(m.topic, body)
				log.Println(string(body))
			}
		}
	}
}
