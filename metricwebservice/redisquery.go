package main

import (
	"github.com/garyburd/redigo/redis"
)

type WebQueryPool struct {
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
