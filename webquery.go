package main

import (
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"net/http"
)

type WebQueryPool struct {
	*Setting
	*redis.Pool
	exitChannel  chan int
	queryChannel chan *RedisQuery
}

type RedisQuery struct {
	Action        string
	Options       []interface{}
	resultChannel chan *QueryResult
}

type QueryResult struct {
	Err   error
	Value interface{}
}

func (q *WebQueryPool) Run() {
	dial := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", q.RedisServer)
		if err != nil {
			return nil, err
		}
		return c, err
	}
	q.Pool = redis.NewPool(dial, 3)
	go q.queryTask()
	r := mux.NewRouter()
	s := r.PathPrefix("/api/v1").Subrouter()

	// /metric
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

	// /host
	s.HandleFunc("/host", HostIndex).
		Methods("GET")
	s.HandleFunc("/host/{name}", HostShow).
		Methods("GET")
	s.HandleFunc("/host/{name}", HostDelete).
		Methods("DELETE")
	// /host/{}/metric
	s.HandleFunc("/host/{host}/metric", HostMetricIndex).
		Methods("GET")
	s.HandleFunc("/host/{host}/metric", MetricUpdate).
		Methods("PATCH").
		Headers("Content-Type", "application/json")
	s.HandleFunc("/host/{host}/metric/{name}", HostMetricDelete).
		Methods("DELETE")
	// /trigger
	s.HandleFunc("/trigger", TriggerCreate).
		Methods("POST").
		Headers("Content-Type", "application/json")
	s.HandleFunc("/trigger/{name}", TriggerShow).
		Methods("GET")
	s.HandleFunc("/trigger/{name}", TriggerDelete).
		Methods("DELETE")
	s.HandleFunc("/triggerhistory/{name}", TriggerHistoryShow).
		Methods("GET")
	// /trigger/{}/action
	s.HandleFunc("/trigger/{trigger}/action", ActionCreate).Methods("POST").Headers("Content-Type", "application/json")
	s.HandleFunc("/trigger/{trigger}/action", ActionIndex).
		Methods("GET")
	s.HandleFunc("/trigger/{trigger}/action/{name}", ActionDelete).Methods("DELETE")

	http.Handle("/", r)
	go http.ListenAndServe(queryservice.ListenAddress, nil)
}

func (q *WebQueryPool) queryTask() {
	con := q.Get()
	defer con.Close()
	for {
		select {
		case <-q.exitChannel:
			return
		case query := <-q.queryChannel:
			value, err := con.Do(query.Action, query.Options...)
			if err != nil && err != redis.ErrNil {
				con.Close()
				con = q.Get()
			}
			query.resultChannel <- &QueryResult{
				Err:   err,
				Value: value,
			}
		}
	}
}

func (q *WebQueryPool) Stop() {
	close(q.exitChannel)
	q.Pool.Close()
}
