package main

import (
	"../.."
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/nsqio/go-nsq"
	"os"
)

type WebService struct {
	NsqdAddress   string `json:"nsqd_addr"`
	MetricTopic   string `json:"metric_topic"`
	RedisServer   string `json:"redis_server"`
	MaxInFlight   int    `json:"maxinflight"`
	ListenAddress string `json:"listen_address"`
	engine        metrictools.StoreEngine
	producer      *nsq.Producer
}

func (m *WebService) Run() error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	cfg := nsq.NewConfig()
	cfg.Set("user_agent", fmt.Sprintf("metric_web-%s/%s", VERSION, hostname))
	cfg.Set("snappy", true)
	cfg.Set("max_in_flight", m.MaxInFlight)
	m.producer, err = nsq.NewProducer(m.NsqdAddress, cfg)
	if err != nil {
		return err
	}
	m.engine = &metrictools.RedisEngine{
		RedisServer: m.RedisServer,
		ExitChannel: make(chan int),
		CmdChannel:  make(chan metrictools.Request),
	}
	taskPool := m.MaxInFlight/100 + 1
	for i := 0; i < taskPool; i++ {
		go m.engine.RunTask()
	}
	r := gin.Default()
	r.Use(m.loginFilter())
	authorized := r.Group("/api/v1")
	authorized.POST("/collect", m.Collectd)
	go r.Run(m.ListenAddress)
	return nil
}
func (m *WebService) Stop() {
	m.producer.Stop()
	m.engine.Stop()
}
