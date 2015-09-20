package main

import (
	"../.."
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/datastream/skyline"
	"github.com/nsqio/go-nsq"
	"log"
	"os"
	"time"
)

// TriggerTask define a trigger statistic task
type SkylineTask struct {
	*metrictools.Setting
	consumer    *nsq.Consumer
	producer    *nsq.Producer
	exitChannel chan int
	engine      metrictools.StoreEngine
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
	m.engine = &metrictools.RedisEngine{
		Setting:     m.Setting,
		ExitChannel: make(chan int),
		CmdChannel:  make(chan interface{}),
	}
	go m.engine.RunTask()
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
	m.engine.Stop()
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
	for {
		select {
		case <-m.exitChannel:
			return
		case msg := <-m.msgChannel:
			rst, last, err := m.SkylineCheck(msg.Body.(string))
			if err == nil {
				threshold := 8 - m.Consensus
				if CheckThreshhold(rst, threshold) {
					log.Println(msg.Body.(string), rst)
					var stat bool
					stat, err = m.CheckHistory(msg.Body.(string), last)
					if stat {
						err = m.SendNotify(msg.Body.(string), last)
					}
				}
			}
			if err != nil && err.Error() == "null data" {
				err = nil
			}
			msg.ErrorChannel <- err
		}
	}
}
func (m *SkylineTask) SendNotify(exp string, last skyline.TimePoint) error {
	rst := make(map[string]string)
	rst["time"] = time.Now().Format("2006-01-02 15:04:05")
	h := sha1.New()
	h.Write([]byte(exp))
	name := base64.URLEncoding.EncodeToString(h.Sum(nil))
	rst["trigger"] = exp
	rst["url"] = "/api/v1/triggerhistory/" + name
	rst["event"] = fmt.Sprintf("%s = %f is anomalous @ %d!", exp, last.GetValue(), last.GetTimestamp())
	body, err := json.Marshal(rst)
	if err == nil {
		m.producer.Publish(m.NotifyTopic, body)
		log.Println(string(body))
	}
	return err
}
func (m *SkylineTask) CheckHistory(exp string, last skyline.TimePoint) (bool, error) {
	reply, err := m.engine.GetValues("trigger_history:" + exp)
	if err != nil {
		return true, err
	}
	rawTriggerHistory := []byte(reply[0])
	var triggerHistory []skyline.TimePoint
	if err = json.Unmarshal(rawTriggerHistory, &triggerHistory); err != nil {
		return true, err
	}
	isan, t := skyline.IsAnomalouslyAnomalous(triggerHistory, last)
	if len(t) > 0 {
		body, err := json.Marshal(t)
		if err == nil {
			err = m.engine.SetKeyValue("trigger_history:"+exp, body)
		}
		if err != nil {
			return false, err
		}
	}
	return isan, nil
}

func (m *SkylineTask) SkylineCheck(exp string) ([]int, skyline.TimePoint, error) {
	t := time.Now().Unix()
	values, err := m.engine.GetValues(fmt.Sprintf("archive:%s:%d", exp, t/14400-8), fmt.Sprintf("archive:%s:%d", exp, t/14400-7), fmt.Sprintf("archive:%s:%d", exp, t/14400-6), fmt.Sprintf("archive:%s:%d", exp, t/14400-5), fmt.Sprintf("archive:%s:%d", exp, t/14400-4), fmt.Sprintf("archive:%s:%d", exp, t/14400-3), fmt.Sprintf("archive:%s:%d", exp, t/14400-2), fmt.Sprintf("archive:%s:%d", exp, t/14400-1), fmt.Sprintf("archive:%s:%d", exp, t/14400))
	var rst []int
	var tp skyline.TimePoint
	if err != nil {
		return rst, tp, err
	}
	timeseries := metrictools.ParseTimeSeries(values)
	if len(timeseries) == 0 {
		log.Println(fmt.Sprintf("archive:%s:%d", exp, t/14400-8), fmt.Sprintf("archive:%s:%d", exp, t/14400-7), fmt.Sprintf("archive:%s:%d", exp, t/14400-6), fmt.Sprintf("archive:%s:%d", exp, t/14400-5), fmt.Sprintf("archive:%s:%d", exp, t/14400-4), fmt.Sprintf("archive:%s:%d", exp, t/14400-3), fmt.Sprintf("archive:%s:%d", exp, t/14400-2), fmt.Sprintf("archive:%s:%d", exp, t/14400-1), fmt.Sprintf("archive:%s:%d", exp, t/14400), "null data")
		return rst, tp, fmt.Errorf("null data")
	}
	if skyline.MedianAbsoluteDeviation(timeseries) {
		rst = append(rst, 1)
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
