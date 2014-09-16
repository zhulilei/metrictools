package main

import (
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/datastream/cal"
	"github.com/datastream/skyline"
	"github.com/fzzy/radix/redis"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

// TriggerTask define a trigger statistic task
type TriggerTask struct {
	*Setting
	client         *redis.Client
	producer       *nsq.Producer
	exitChannel    chan int
	triggerChannel chan string
}

func (m *TriggerTask) Run() error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	cfg := nsq.NewConfig()
	cfg.Set("user_agent", fmt.Sprintf("metric_trigger/%s", hostname))
	cfg.Set("snappy", true)
	cfg.Set("max_in_flight", m.MaxInFlight)
	m.producer, err = nsq.NewProducer(m.NsqdAddress, cfg)
	if err == nil {
		go m.ScanTrigger()
		go m.calculateTask()
	}
	return err
}

func (m *TriggerTask) Stop() {
	close(m.exitChannel)
	m.producer.Stop()
}

// ScanTrigger will find out all trigger which not updated in 60s
func (m *TriggerTask) ScanTrigger() {
	ticker := time.Tick(time.Second * 30)
	client, err := redis.Dial(m.Network, m.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
	}
	defer client.Close()
	for {
		select {
		case <-ticker:
			reply := client.Cmd("SMEMBERS", "triggers")
			if reply.Err != nil {
				client.Close()
				log.Println("redis connection close")
				client, _ = redis.Dial(m.Network, m.RedisServer)
				continue
			}
			keys, _ := reply.List()
			now := time.Now().Unix()
			for _, v := range keys {
				reply = client.Cmd("HGET", v, "last")
				if reply.Err != nil {
					client.Close()
					log.Println("redis connection close")
					client, _ = redis.Dial(m.Network, m.RedisServer)
					continue
				}
				last, err := reply.Int64()
				if err != nil {
					continue
				}
				if now-last < 61 {
					continue
				}
				m.triggerChannel <- v
			}
		case <-m.exitChannel:
			return
		}
	}
}

func (m *TriggerTask) calculateTask() {
	client, _ := redis.Dial(m.Network, m.RedisServer)
	defer client.Close()
	for {
		select {
		case <-m.exitChannel:
			return
		case name := <-m.triggerChannel:
			now := time.Now().Unix()
			if reply := client.Cmd("HSET", name, "last", now); reply.Err != nil {
				client.Close()
				client, _ = redis.Dial(m.Network, m.RedisServer)
				continue
			}
			err := m.calculate(name, client)
			log.Println("calculate failed", err)
		}
	}
}

func (m *TriggerTask) calculate(triggerName string, client *redis.Client) error {
	var trigger Trigger
	var err error
	reply := client.Cmd("HGET", triggerName, "is_e")
	if reply.Err != nil {
		log.Println("get trigger failed:", triggerName, err)
		return reply.Err
	}
	trigger.IsExpression, err = reply.Bool()
	trigger.Name = triggerName
	if trigger.IsExpression {
		expList := cal.Parser(trigger.Name)
		v, err := m.evalExp(expList, client)
		if err != nil {
			log.Println("calculate failed:", trigger.Name, err)
			return err
		}
		t := time.Now().Unix()
		body, err := KeyValueEncode(t, v)
		if err != nil {
			log.Println("encode data failed:", err)
			return err
		}
		client.Append("APPEND", fmt.Sprintf("archive:%s:%d", trigger.Name, t/14400), body)
		// ttl ?
		client.Append("EXPIRE", fmt.Sprintf("archive:%s:%d", trigger.Name, t/14400), 90000)
		client.GetReply()
		reply = client.GetReply()
		if reply.Err != nil {
			return reply.Err
		}
	}
	return m.checkvalue(trigger.Name, trigger.IsExpression, client)
}

// ParseTimeSeries convert redis value to skyline's data format
func ParseTimeSeries(values []string) []skyline.TimePoint {
	var rst []skyline.TimePoint
	for _, val := range values {
		size := len(val)
		for i := 0; i < size; i += 18 {
			if (i + 18) > size {
				break
			}
			kv, err := KeyValueDecode([]byte(val[i : i+18]))
			if err != nil {
				continue
			}
			rst = append(rst, &kv)
		}
	}
	return rst
}

func (m *TriggerTask) evalExp(expList []string, client *redis.Client) (float64, error) {
	kv := make(map[string]interface{})
	exp := ""
	current := time.Now().Unix()
	for _, item := range expList {
		var v float64
		reply := client.Cmd("HMGET", item, "rate_value", "timestamp")
		if reply.Err != nil {
			return 0, reply.Err
		}
		values, err := reply.List()
		v, err = strconv.ParseFloat(item, 64)
		if err != nil {
			v, _ = strconv.ParseFloat(values[0], 64)
		}
		t, err := strconv.ParseInt(values[1], 0, 64)
		if err != nil {
			t = time.Now().Unix()
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

func (m *TriggerTask) checkvalue(exp string, isExpression bool, client *redis.Client) error {
	t := time.Now().Unix()
	reply := client.Cmd("MGET", fmt.Sprintf("archive:%s:%d", exp, t/14400-7), fmt.Sprintf("archive:%s:%d", exp, t/14400-6), fmt.Sprintf("archive:%s:%d", exp, t/14400-5), fmt.Sprintf("archive:%s:%d", exp, t/14400-4), fmt.Sprintf("archive:%s:%d", exp, t/14400-3), fmt.Sprintf("archive:%s:%d", exp, t/14400-2), fmt.Sprintf("archive:%s:%d", exp, t/14400-1), fmt.Sprintf("archive:%s:%d", exp, t/14400))
	if reply.Err != nil {
		return nil
	}
	values, err := reply.List()
	var skylineTrigger []string
	threshold := 8 - m.Consensus
	if err != nil {
	}
	timeseries := ParseTimeSeries(values)
	if len(timeseries) == 0 {
		log.Println(exp, " is empty")
		return nil
	}
	timeSize := timeseries[len(timeseries)-1].GetTimestamp() - timeseries[0].GetTimestamp()
	if timeSize < m.FullDuration {
		log.Println("incomplete data", exp, timeSize)
		return nil
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
		reply := client.Cmd("GET", "trigger_history:"+exp)
		raw_trigger_history, _ := reply.Bytes()
		var trigger_history []skyline.TimePoint
		if err := json.Unmarshal(raw_trigger_history, &trigger_history); err != nil {
			return err
		}
		isan, t := skyline.IsAnomalouslyAnomalous(trigger_history, timeseries[len(timeseries)-1])
		if len(t) > 0 {
			body, err := json.Marshal(t)
			if err == nil {
				reply := client.Cmd("SET", "trigger_history:"+exp, body)
				err = reply.Err
			}
			if err != nil {
				return err
			}
		}
		if isan {
			rst := make(map[string]string)
			rst["time"] = time.Now().Format("2006-01-02 15:04:05")
			rst["event"] = strings.Join(skylineTrigger, ", ")
			h := sha1.New()
			h.Write([]byte(exp))
			name := base64.URLEncoding.EncodeToString(h.Sum(nil))
			rst["trigger"] = name
			rst["trigger_exp"] = exp
			rst["url"] = "/api/v1/triggerhistory/" + name
			if body, err := json.Marshal(rst); err == nil {
				m.producer.Publish(m.NotifyTopic, body)
				log.Println(string(body))
			}
		}
	}
	if len(skylineTrigger) > 4 {
		log.Println(skylineTrigger, exp)
	}
	return nil
}
