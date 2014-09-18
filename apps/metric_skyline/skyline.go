package main

import (
	"../.."
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/datastream/skyline"
	"github.com/fzzy/radix/redis"
	"log"
	"os"
	"time"
)

// TriggerTask define a trigger statistic task
type SkylineTask struct {
	*metrictools.Setting
	client      *redis.Client
	consumer    *nsq.Consumer
	producer    *nsq.Producer
	exitChannel chan int
	msgChannel  chan *metrictools.Message
}

func (m *SkylineTask) Run() error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	cfg := nsq.NewConfig()
	cfg.Set("user_agent", fmt.Sprintf("metric_skyline/%s", hostname))
	cfg.Set("snappy", true)
	cfg.Set("max_in_flight", m.MaxInFlight)
	m.producer, err = nsq.NewProducer(m.NsqdAddress, cfg)
	if err != nil {
		return err
	}
	m.consumer, err = nsq.NewConsumer(m.SkylineTopic, m.SkylineChannel, cfg)
	if err != nil {
		return err
	}
	m.consumer.AddConcurrentHandlers(m, m.MaxInFlight)
	err = m.consumer.ConnectToNSQLookupds(m.LookupdAddresses)
	if err != nil {
		return err
	}
	go m.SkylineCalculateTask()
	return nil
}
func (m *SkylineTask) Stop() {
	m.producer.Stop()
	close(m.exitChannel)
	m.consumer.Stop()
}

func (m *SkylineTask) HandleMessage(msg *nsq.Message) error {
	message := &metrictools.Message{
		Body:         string(msg.Body),
		ErrorChannel: make(chan error),
	}
	m.msgChannel <- message
	return <-message.ErrorChannel
}

func (m *SkylineTask) SkylineCalculateTask() {
	client, _ := redis.Dial(m.Network, m.RedisServer)
	defer client.Close()
	for {
		select {
		case <-m.exitChannel:
			return
		case msg := <-m.msgChannel:
			rst, last, err := m.SkylineCheck(msg.Body.(string), client)
			if err == nil {
				threshold := 8 - m.Consensus
				if CheckThreshhold(rst, threshold) {
					var stat bool
					stat, err = CheckHistory(msg.Body.(string), last, client)
					if stat {
						err = m.SendNotify(msg.Body.(string))
					}
				}
			}
			msg.ErrorChannel <- err
		}
	}
}
func (m *SkylineTask) SendNotify(exp string) error {
	rst := make(map[string]string)
	rst["time"] = time.Now().Format("2006-01-02 15:04:05")
	h := sha1.New()
	h.Write([]byte(exp))
	name := base64.URLEncoding.EncodeToString(h.Sum(nil))
	rst["trigger"] = name
	rst["url"] = "/api/v1/triggerhistory/" + name
	body, err := json.Marshal(rst)
	if err == nil {
		m.producer.Publish(m.NotifyTopic, body)
		log.Println(string(body))
	}
	return err
}
func CheckHistory(exp string, last skyline.TimePoint, client *redis.Client) (bool, error) {
	reply := client.Cmd("GET", "trigger_history:"+exp)
	raw_trigger_history, _ := reply.Bytes()
	var trigger_history []skyline.TimePoint
	if err := json.Unmarshal(raw_trigger_history, &trigger_history); err != nil {
		return false, err
	}
	isan, t := skyline.IsAnomalouslyAnomalous(trigger_history, last)
	if len(t) > 0 {
		body, err := json.Marshal(t)
		if err == nil {
			reply := client.Cmd("SET", "trigger_history:"+exp, body)
			err = reply.Err
		}
		if err != nil {
			return false, err
		}
	}
	return isan, nil
}

func (m *SkylineTask) SkylineCheck(exp string, client *redis.Client) ([]int, skyline.TimePoint, error) {
	t := time.Now().Unix()
	reply := client.Cmd("MGET", fmt.Sprintf("archive:%s:%d", exp, t/14400-7), fmt.Sprintf("archive:%s:%d", exp, t/14400-6), fmt.Sprintf("archive:%s:%d", exp, t/14400-5), fmt.Sprintf("archive:%s:%d", exp, t/14400-4), fmt.Sprintf("archive:%s:%d", exp, t/14400-3), fmt.Sprintf("archive:%s:%d", exp, t/14400-2), fmt.Sprintf("archive:%s:%d", exp, t/14400-1), fmt.Sprintf("archive:%s:%d", exp, t/14400))
	var rst []int
	var tp skyline.TimePoint
	if reply.Err != nil {
		return rst, tp, reply.Err
	}
	values, _ := reply.List()
	timeseries := metrictools.ParseTimeSeries(values)
	if len(timeseries) == 0 {
		return rst, tp, fmt.Errorf("null data")
	}
	if skyline.MedianAbsoluteDeviation(timeseries) {
	} else {
		rst = append(rst, 0)
	}
	if skyline.Grubbs(timeseries) {
		rst = append(rst, 1)
	} else {
		rst = append(rst, 0)
	}
	if skyline.FirstHourAverage(timeseries, m.FullDuration) {
		rst = append(rst, 1)
	} else {
		rst = append(rst, 0)
	}
	if skyline.SimpleStddevFromMovingAverage(timeseries) {
		rst = append(rst, 1)
	} else {
		rst = append(rst, 0)
	}
	if skyline.StddevFromMovingAverage(timeseries) {
		rst = append(rst, 1)
	} else {
		rst = append(rst, 0)
	}
	if skyline.MeanSubtractionCumulation(timeseries) {
		rst = append(rst, 1)
	} else {
		rst = append(rst, 0)
	}
	if skyline.LeastSquares(timeseries) {
		rst = append(rst, 1)
	} else {
		rst = append(rst, 0)
	}
	if skyline.HistogramBins(timeseries) {
		rst = append(rst, 1)
	} else {
		rst = append(rst, 0)
	}
	return rst, timeseries[len(timeseries)-1], nil
}

func CheckThreshhold(data []int, threshold int) bool {
	stat := 0
	for _, v := range data {
		if v == 1 {
			stat++
		}
	}
	if (len(data) - stat) <= threshold {
		return true
	}
	return false
}
