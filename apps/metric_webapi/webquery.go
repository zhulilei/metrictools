package main

import (
	"../.."
	"fmt"
	"github.com/gorilla/mux"
	"github.com/nsqio/go-nsq"
	"net/http"
	"os"
)

type WebService struct {
	*metrictools.Setting
	engine   metrictools.StoreEngine
	producer *nsq.Producer
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
		Setting:     m.Setting,
		ExitChannel: make(chan int),
		CmdChannel:  make(chan metrictools.Request),
	}
	taskPool := m.MaxInFlight/100 + 1
	for i := 0; i < taskPool; i++ {
		go m.engine.RunTask()
	}
	r := mux.NewRouter()
	s := r.PathPrefix("/api/v1").Subrouter()

	s.HandleFunc("/collect", m.Collectd).
		Methods("POST").
		Headers("Content-Type", "application/json")
	http.Handle("/", r)
	http.ListenAndServe(m.ListenAddress, nil)
	return nil
}
func (m *WebService) Stop() {
	m.producer.Stop()
	m.engine.Stop()
}
