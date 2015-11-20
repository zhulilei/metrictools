package main

import (
	"../.."
	"errors"
	"fmt"
	"github.com/datastream/cal"
	"github.com/influxdb/influxdb/client/v2"
	"github.com/nsqio/go-nsq"
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
	cfg.Set("user_agent", fmt.Sprintf("metric_statistic-%s/%s", VERSION, hostname))
	cfg.Set("snappy", true)
	cfg.Set("max_in_flight", m.MaxInFlight)
	m.engine = &metrictools.RedisEngine{
		Setting:     m.Setting,
		ExitChannel: make(chan int),
		CmdChannel:  make(chan interface{}),
	}
	go m.engine.RunTask()
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
// may be can be replace with 'select value/10 from series where tag=a '
func (m *StatisticTask) calculateTask() {
	db, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:      m.InfluxdbAddress,
		Username:  m.InfluxdbUser,
		Password:  m.InfluxdbPassword,
		UserAgent: fmt.Sprintf("metrictools-%s", VERSION),
	})
	if err != nil {
		log.Println("NewHTTPClient error:", err)
	}
	defer db.Close()
	q := client.NewQuery(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", m.InfluxdbDatabase), "", "s")
	if response, err := db.Query(q); err == nil && response.Error() == nil {
		log.Fatal("create influxdb database failed:", response.Results)
	}
	for {
		bp, _ := client.NewBatchPoints(client.BatchPointsConfig{
			Database:  m.InfluxdbDatabase,
			Precision: "s",
		})
		select {
		case <-m.exitChannel:
			return
		case name := <-m.statisticChannel:
			trigger, err := m.engine.GetTrigger(name)
			if trigger.IsExpression && err == nil {
				value, err := m.calculate(trigger)
				if err != nil {
					log.Println("calculate failed", err)
				}
				fields := make(map[string]interface{})
				fields["value"] = value
				tags := make(map[string]string)
				tags["name"] = name
				tags["user"] = trigger.Owner
				pt, err := client.NewPoint("triggers", tags, fields, time.Now())
				if err != nil {
					log.Println("NewPoint Error:", err)
					break
				}
				bp.AddPoint(pt)

			}
			m.producer.Publish(m.SkylineTopic, []byte(name))
		}
		if err == nil {
			err = db.Write(bp)
		}
	}
}

func (m *StatisticTask) calculate(tg metrictools.Trigger) (float64, error) {
	expList := cal.Parser(tg.Name)
	kv := make(map[string]interface{})
	var exp string
	current := time.Now().Unix()
	for _, item := range expList {
		metricKey := item
		if len(item) > 2 && item[0] == '"' && item[len(item)-1]=='"' {
			metricKey = metricKey[1 : len(metricKey)-1]
		}
		metric, err := m.engine.GetMetric(metricKey)
		v := metric.RateValue
		t := metric.LastTimestamp
		if err != nil {
			return 0, err
		}
		if (current - t) > 60 {
			return 0, errors.New(item + "'s data are too old")
		}
		kv[item] = v
		exp += item
	}
	rst, err := cal.Cal(exp, kv)
	return rst, err
}
