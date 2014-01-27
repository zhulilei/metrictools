package main

import (
	"encoding/base64"
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"regexp"
	"strings"
)

// TriggerShow  GET /trigger/{:name}
func TriggerShow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	tg := mux.Vars(r)["name"]
	starttime := r.FormValue("starttime")
	endtime := r.FormValue("endtime")
	start := gettime(starttime)
	end := gettime(endtime)
	if !checktime(start, end) {
		start = end - 3600*3
	}
	n, err := base64.URLEncoding.DecodeString(tg)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	name := string(n)
	q := &RedisQuery{
		Action:        "HGET",
		Options:       []interface{}{name, "is_e"},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult := <-q.resultChannel
	_, err = redis.String(queryresult.Value, queryresult.Err)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		var recordList []interface{}
		q := &RedisQuery{
			Action:        "ZRANGEBYSCORE",
			Options:       []interface{}{"archive:" + name, start, end},
			resultChannel: make(chan *QueryResult),
		}
		queryservice.queryChannel <- q
		queryresult := <-q.resultChannel
		metricData, err := redis.Strings(queryresult.Value, queryresult.Err)
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		record := make(map[string]interface{})
		tgname := base64.URLEncoding.EncodeToString([]byte(name))
		record["name"] = tgname
		record["values"] = GenerateTimeseries(metricData)
		recordList = append(recordList, record)
		rst := make(map[string]interface{})
		rst["metrics"] = recordList
		rst["url"] = "/api/v1/trigger/" + tgname
		if body, err := json.Marshal(rst); err == nil {
			w.Write(body)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

// TriggerCreate POST /trigger
func TriggerCreate(w http.ResponseWriter, r *http.Request) {
	var tg Trigger
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&tg); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	tg.Name = strings.Trim(tg.Name, " ")
	tg.IsExpression, _ = regexp.MatchString(`(\+|-|\*|/)`, tg.Name)
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	q := &RedisQuery{
		Action:        "HGET",
		Options:       []interface{}{tg.Name, "role"},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult := <-q.resultChannel
	_, err := redis.String(queryresult.Value, queryresult.Err)
	if err == nil {
		w.WriteHeader(http.StatusNotAcceptable)
		w.Write([]byte(tg.Name + " exists"))
		return
	}
	q = &RedisQuery{
		Action:        "HMSET",
		Options:       []interface{}{tg.Name, "is_e", tg.IsExpression, "role", tg.Role},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	<-q.resultChannel
	q = &RedisQuery{
		Action:        "SADD",
		Options:       []interface{}{"triggers", tg.Name},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult = <-q.resultChannel
	if queryresult.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed insert"))
	} else {
		t := make(map[string]string)
		t["name"] = base64.URLEncoding.EncodeToString([]byte(tg.Name))
		t["url"] = "/api/v1/trigger/" + t["name"]
		if body, err := json.Marshal(t); err == nil {
			w.Write(body)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

// TriggerDelete DELETE /trigger/{:name}
func TriggerDelete(w http.ResponseWriter, r *http.Request) {
	tg := mux.Vars(r)["name"]
	n, err := base64.URLEncoding.DecodeString(tg)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	name := string(n)
	q := &RedisQuery{
		Action:        "HGET",
		Options:       []interface{}{name, "is_e"},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult := <-q.resultChannel
	isExpression, err := redis.Bool(queryresult.Value, queryresult.Err)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if isExpression {
		q := &RedisQuery{
			Action:        "DEL",
			Options:       []interface{}{"archive:" + name},
			resultChannel: make(chan *QueryResult),
		}
		queryservice.queryChannel <- q
		<-q.resultChannel
		q = &RedisQuery{
			Action:        "DEL",
			Options:       []interface{}{name},
			resultChannel: make(chan *QueryResult),
		}
		queryservice.queryChannel <- q
		queryresult = <-q.resultChannel
		if queryresult.Err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	} else {
		q = &RedisQuery{
			Action:        "HDEL",
			Options:       []interface{}{name, "is_e", "role"},
			resultChannel: make(chan *QueryResult),
		}
		queryservice.queryChannel <- q
		<-q.resultChannel
	}
	q = &RedisQuery{
		Action:        "SMEMBERS",
		Options:       []interface{}{name + ":actions"},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult = <-q.resultChannel
	keys, err := redis.Strings(queryresult.Value, queryresult.Err)
	for _, v := range keys {
		q = &RedisQuery{
			Action:        "DEL",
			Options:       []interface{}{v},
			resultChannel: make(chan *QueryResult),
		}
		queryservice.queryChannel <- q
		queryresult = <-q.resultChannel
		if queryresult.Err != nil {
			break
		}
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to delete trigger"))
		return
	}
	q = &RedisQuery{
		Action:        "SREM",
		Options:       []interface{}{"triggers", name},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult = <-q.resultChannel
	if queryresult.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to delete trigger"))
	} else {
		w.Write([]byte("delete successful"))
	}
}
