package main

import (
	"../.."
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/gorilla/mux"
	"net/http"
	"os"
)

type WebService struct {
	*metrictools.Setting
	engine   metrictools.StoreEngine
	producer *nsq.Producer
}

func (q *WebService) Run() error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	cfg := nsq.NewConfig()
	cfg.Set("user_agent", fmt.Sprintf("metric_web/%s", hostname))
	cfg.Set("snappy", true)
	cfg.Set("max_in_flight", q.MaxInFlight)
	q.producer, err = nsq.NewProducer(q.NsqdAddress, cfg)
	if err != nil {
		return err
	}
	q.engine = &metrictools.RedisEngine{
		Setting:     q.Setting,
		ExitChannel: make(chan int),
		CmdChannel:  make(chan interface{}),
	}
	taskPool := q.MaxInFlight/100 +1
	for i := 0; i < taskPool; i++ {
		go q.engine.RunTask()
	}
	r := mux.NewRouter()
	s := r.PathPrefix("/api/v1").Subrouter()

	s.HandleFunc("/collect", q.Collectd).
		Methods("POST").
		Headers("Content-Type", "application/json")
	// /metric
	s.HandleFunc("/metric", q.MetricIndex).
		Methods("GET")
	s.HandleFunc("/metric", q.MetricCreate).
		Methods("POST").
		Headers("Content-Type", "application/json")
	s.HandleFunc("/metric/{name}", q.MetricShow).
		Methods("GET")
	s.HandleFunc("/metric/{name}", q.MetricUpdate).
		Methods("PATCH").
		Headers("Content-Type", "application/json")

	// /host
	s.HandleFunc("/host", q.HostIndex).
		Methods("GET")
	s.HandleFunc("/host/{name}", q.HostShow).
		Methods("GET")
	s.HandleFunc("/host/{name}", q.HostDelete).
		Methods("DELETE")
	// /host/{}/metric
	s.HandleFunc("/host/{host}/metric", q.HostMetricIndex).
		Methods("GET")
	s.HandleFunc("/host/{host}/metric", q.MetricUpdate).
		Methods("PATCH").
		Headers("Content-Type", "application/json")
	s.HandleFunc("/host/{host}/metric/{name}", q.HostMetricDelete).
		Methods("DELETE")
	// /trigger
	s.HandleFunc("/trigger", q.TriggerCreate).
		Methods("POST").
		Headers("Content-Type", "application/json")
	s.HandleFunc("/trigger/{name}", q.TriggerShow).
		Methods("GET")
	s.HandleFunc("/trigger/{name}", q.TriggerDelete).
		Methods("DELETE")
	s.HandleFunc("/triggerhistory/{name}", q.TriggerHistoryShow).
		Methods("GET")
	// /trigger/{}/action
	s.HandleFunc("/trigger/{trigger}/action", q.ActionCreate).Methods("POST").Headers("Content-Type", "application/json")
	s.HandleFunc("/trigger/{trigger}/action", q.ActionIndex).
		Methods("GET")
	s.HandleFunc("/trigger/{trigger}/action/{name}", q.ActionDelete).Methods("DELETE")

	http.Handle("/", r)
	http.ListenAndServe(q.ListenAddress, nil)
	return nil
}
func (q *WebService) Stop() {
	q.producer.Stop()
	q.engine.Stop()
}
