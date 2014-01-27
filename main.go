package main

import (
	"flag"
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

var (
	confFile = flag.String("c", "metrictools.json", "metrictools config file")
)

var queryservice *WebQueryPool

type stringArray []string

func (s *stringArray) Set(value string) error {
	*s = append(*s, value)
	return nil
}
func (s *stringArray) String() string {
	return fmt.Sprintf("%s", *s)
}

var runModes stringArray

type MetricTask interface {
	Stop()
}

func main() {
	flag.Var(&runModes, "m", "List of node modes: archive|notify|proccess|statistic|webapi")
	flag.Parse()
	c, err := ReadConfig(*confFile)
	if err != nil {
		log.Fatal("parse config file error", err)
	}

	if len(runModes) < 1 {
		log.Fatal("need run mode", err)
	}
	var tasks []MetricTask
	for _, v := range runModes {
		switch v {
		case "archive":
			a := &DataArchive{
				Setting:     c,
				msgChannel:  make(chan *Message),
				exitChannel: make(chan int),
			}
			if err := a.Run(); err != nil {
				log.Fatal("fail to run metric archive task", err)
			}
			tasks = append(tasks, a)
		case "notify":
			n := &Notify{
				Setting:     c,
				msgChannel:  make(chan *Message),
				exitChannel: make(chan int),
			}
			if err := n.Run(); err != nil {
				log.Fatal("fail to run metric notify task", err)
			}
			tasks = append(tasks, n)
		case "proccess":
			p := &MetricDeliver{
				Setting:     c,
				exitChannel: make(chan int),
				msgChannel:  make(chan *Message),
			}
			if err := p.Run(); err != nil {
				log.Fatal("fail to run metric proccess task", err)
			}
			tasks = append(tasks, p)
		case "statistic":
			s := &TriggerTask{
				Setting:     c,
				exitChannel: make(chan int),
				msgChannel:  make(chan *Message),
			}
			if err := s.Run(); err != nil {
				log.Fatal("fail to run metric statistic task", err)
			}
		case "webapi":
			queryservice = &WebQueryPool{
				Setting:      c,
				exitChannel:  make(chan int),
				queryChannel: make(chan *RedisQuery),
			}
			go queryservice.Run()
			r := mux.NewRouter()
			s := r.PathPrefix("/api/v1").Subrouter()

			s.HandleFunc("/metric", MetricIndex).
				Methods("GET")

			s.HandleFunc("/metric", MetricCreate).
				Methods("POST").
				Headers("Content-Type", "application/json")

			s.HandleFunc("/metric/{name}", MetricShow).
				Methods("GET")

			s.HandleFunc("/metric/{name}", MetricUpdate).
				Methods("PATCH").
				Headers("Content-Type", "application/json")

			s.HandleFunc("/metric/{name}", MetricDelete).
				Methods("DELETE")

			s.HandleFunc("/host", HostIndex).
				Methods("GET")
			s.HandleFunc("/host/{name}", HostShow).
				Methods("GET")
			s.HandleFunc("/host/{name}", HostDelete).
				Methods("DELETE")

			s.HandleFunc("/host/{host}/metric", HostMetricIndex).
				Methods("GET")
			s.HandleFunc("/host/{host}/metric", MetricUpdate).
				Methods("PATCH").
				Headers("Content-Type", "application/json")
			s.HandleFunc("/host/{host}/metric/{name}", HostMetricDelete).Methods("DELETE")

			s.HandleFunc("/trigger", TriggerCreate).
				Methods("POST").
				Headers("Content-Type", "application/json")
			s.HandleFunc("/trigger/{name}", TriggerShow).
				Methods("GET")
			s.HandleFunc("/trigger/{name}", TriggerDelete).
				Methods("DELETE")

			s.HandleFunc("/trigger/{trigger}/action", ActionCreate).Methods("POST").Headers("Content-Type", "application/json")
			s.HandleFunc("/trigger/{trigger}/action", ActionIndex).
				Methods("GET")
			s.HandleFunc("/trigger/{trigger}/action/{name}", ActionDelete).Methods("DELETE")

			http.Handle("/", r)
			go http.ListenAndServe(queryservice.ListenAddress, nil)
		default:
			log.Println(v, "is not supported mode")
		}
	}

	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	for _, t := range tasks {
		t.Stop()
	}
}
