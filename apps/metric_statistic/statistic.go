package main

import (
	"../.."
	"errors"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/datastream/cal"
	"log"
	"os"
	"time"
)

// StatisticTask define a statistic task
type StatisticTask struct {
	*metrictools.Setting
	producer         *nsq.Producer
	engine           metrictools.StoreEngine
	exitChannel      chan int
	statisticChannel chan string
}

func (m *StatisticTask) Run() error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	cfg := nsq.NewConfig()
	cfg.Set("user_agent", fmt.Sprintf("metric_statistic/%s", hostname))
	cfg.Set("snappy", true)
	cfg.Set("max_in_flight", m.MaxInFlight)
	m.engine = &metrictools.RedisEngine{Setting: m.Setting}
	go m.engine.Start()
	m.producer, err = nsq.NewProducer(m.NsqdAddress, cfg)
	if err == nil {
		go m.ScanTrigger()
		go m.calculateTask()
	}
	return err
}

func (m *StatisticTask) Stop() {
	close(m.exitChannel)
	m.producer.Stop()
	m.engine.Stop()
}

// ScanStattistic will find out all trigger which not updated in 60s
func (m *StatisticTask) ScanTrigger() {
	ticker := time.Tick(time.Second * 30)
	for {
		select {
		case <-ticker:
			keys, err := m.engine.GetSet("triggers")
			if err != nil {
				continue
			}
			now := time.Now().Unix()
			for _, v := range keys {
				trigger, err := m.engine.GetTrigger(v)
				if err != nil {
					continue
				}
				last := trigger.LastTime
				if err != nil {
					continue
				}
				if now-last < 61 {
					continue
				}
				m.statisticChannel <- v
			}
		case <-m.exitChannel:
			return
		}
	}
}

func (m *StatisticTask) calculateTask() {
	for {
		select {
		case <-m.exitChannel:
			return
		case name := <-m.statisticChannel:
			trigger, err := m.engine.GetTrigger(name)
			if trigger.IsExpression && err == nil {
				err := m.calculate(name)
				log.Println("calculate failed", err)
			}
			m.producer.Publish(m.SkylineTopic, []byte(name))
		}
	}
}

func (m *StatisticTask) calculate(exp string) error {
	expList := cal.Parser(exp)
	v, err := m.evalExp(expList)
	if err != nil {
		log.Println("calculate failed:", exp, err)
		return err
	}
	t := time.Now().Unix()
	body, err := metrictools.KeyValueEncode(t, v)
	if err != nil {
		log.Println("encode data failed:", err)
		return err
	}
	m.engine.AppendKeyValue(fmt.Sprintf("archive:%s:%d", exp, t/14400), body)
	err = m.engine.SetTTL(fmt.Sprintf("archive:%s:%d", exp, t/14400), 90000)
	return err
}

func (m *StatisticTask) evalExp(expList []string) (float64, error) {
	kv := make(map[string]interface{})
	exp := ""
	current := time.Now().Unix()
	for _, item := range expList {
		metric, err := m.engine.GetMetric(item)
		v := metric.RateValue
		t := metric.LastTimestamp
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
