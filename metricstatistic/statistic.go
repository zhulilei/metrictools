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
	*redis.Pool
	writer              *nsq.Writer
	reader              *nsq.Reader
	maxInFlight         int
	triggerTopic        string
	triggerChannel      string
	nsqdAddress         string
	notifyTopic         string
	nsqdAddr            string
	nsqlookupdAddresses []string
	exitChannel         chan int
	msgChannel          chan *metrictools.Message
	FullDuration        int64
	Consensus           int
}

func (m *TriggerTask) Run() error {
	var err error
	m.reader, err = nsq.NewReader(m.triggerTopic, m.triggerChannel)
	if err != nil {
		return err
	}
	m.reader.SetMaxInFlight(m.maxInFlight)
	m.writer = nsq.NewWriter(m.nsqdAddr)
	for i := 0; i < m.maxInFlight; i++ {
		m.reader.AddHandler(m)
		go m.calculateTask()
	}
	for _, addr := range m.nsqlookupdAddresses {
		err = m.reader.ConnectToLookupd(addr)
		if err != nil {
			return err
		}
	}
	return err
}

func (m *TriggerTask) Stop() {
	m.reader.Stop()
	close(m.exitChannel)
	m.writer.Stop()
}

// HandleMessage is TriggerTask's nsq handle function
func (m *TriggerTask) HandleMessage(msg *nsq.Message) error {
	message := &metrictools.Message{
		Body:         string(msg.Body),
		ErrorChannel: make(chan error),
	}
	m.msgChannel <- message
	return <-message.ErrorChannel
}

func (m *TriggerTask) calculateTask() {
	con := m.Get()
	defer con.Close()
	for {
		select {
		case <-m.exitChannel:
			return
		case msg := <-m.msgChannel:
			now := time.Now().Unix()
			name, ok := msg.Body.(string)
			if !ok {
				fmt.Println("message error:", msg.Body)
			}
			if _, err := con.Do("HSET", name, "last", now); err != nil {
				con.Close()
				con = m.Get()
				msg.ErrorChannel <- err
			}
			err := m.calculate(name, con)
			msg.ErrorChannel <- err
		}
	}
}

func (m *TriggerTask) calculate(triggerName string, con redis.Conn) error {
	var trigger metrictools.Trigger
	var err error
	trigger.IsExpression, err = redis.Bool(con.Do("HGET", triggerName, "is_e"))
	if err != nil {
		log.Println("get trigger failed:", triggerName, err)
		return err
	}
	trigger.Name = triggerName
	if trigger.IsExpression {
		expList := cal.Parser(trigger.Name)
		v, err := m.evalExp(expList, con)
		if err != nil {
			log.Println("calculate failed:", trigger.Name, err)
			return err
		}
		t := time.Now().Unix()
		body := fmt.Sprintf("%d:%.2f", t, v)
		_, err = con.Do("ZADD", "archive:"+trigger.Name, t, body)
		_, err = con.Do("ZREMRANGEBYSCORE", "archive:"+trigger.Name, 0, t-m.FullDuration-600)
		if err != nil {
			return err
		}
	}
	return m.checkvalue(trigger.Name, trigger.IsExpression, con)
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

func (m *TriggerTask) evalExp(expList []string, con redis.Conn) (float64, error) {
	kv := make(map[string]interface{})
	exp := ""
	current := time.Now().Unix()
	for _, item := range expList {
		var v float64
		t := time.Now().Unix()
		values, err := redis.Values(con.Do("HMGET", item, "rate_value", "timestamp"))
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

func (m *TriggerTask) checkvalue(exp string, isExpression bool, con redis.Conn) error {
	t := time.Now().Unix()
	values, err := redis.Strings(con.Do("ZRANGEBYSCORE", "archive:"+exp, t-m.FullDuration-120, t))
	var skylineTrigger []string
	threshold := 8 - m.Consensus
	if err != nil {
		return nil
	}
	timeseries := ParseTimeSeries(values)
	if len(timeseries) == 0 {
		log.Println(exp, " is empty")
		return nil
	}
	timeSize := timeseries[len(timeseries)-1].Timestamp - timeseries[0].Timestamp
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
			m.writer.Publish(m.notifyTopic, body)
			log.Println(string(body))
		}
	}
	if len(skylineTrigger) > 4 {
		log.Println(skylineTrigger, exp)
	}
	return nil
}
