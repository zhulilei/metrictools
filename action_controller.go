package main

import (
	"encoding/base64"
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

// ActionIndex GET /trigger/{:triggername}/action
func ActionIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	trigger := mux.Vars(r)["trigger"]
	t, err := base64.URLEncoding.DecodeString(trigger)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	tg := string(t)
	q := &RedisQuery{
		Action:        "SMEMBERS",
		Options:       []interface{}{tg + ":actions"},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult := <-q.resultChannel
	if queryresult.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		body, _ := json.Marshal(queryresult.Value)
		w.Write(body)
	}
}

// ActionCreate POST /trigger/{:triggername}/action
func ActionCreate(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var action NotifyAction
	if err := json.NewDecoder(r.Body).Decode(&action); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	trigger := mux.Vars(r)["trigger"]
	t, err := base64.URLEncoding.DecodeString(trigger)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	tg := string(t)
	q := &RedisQuery{
		Action:        "HGET",
		Options:       []interface{}{tg, "role"},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult := <-q.resultChannel
	if _, err := redis.String(queryresult.Value, queryresult.Err); err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	name := base64.URLEncoding.EncodeToString([]byte(action.Uri))
	q = &RedisQuery{
		Action:        "HMSET",
		Options:       []interface{}{tg + ":" + name, "repeat", action.Repeat, "uri", action.Uri},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	<-q.resultChannel

	q = &RedisQuery{
		Action:        "SADD",
		Options:       []interface{}{tg + ":actions", tg + ":" + name},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult = <-q.resultChannel
	if queryresult.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed insert"))
	} else {
		t := make(map[string]string)
		t["trigger_name"] = tg
		t["action_name"] = base64.URLEncoding.EncodeToString([]byte(name))
		t["url"] = "/api/v1/trigger/" + trigger + "/" + name
		if body, err := json.Marshal(t); err == nil {
			w.Write(body)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

// ActionDelete DELETE /trigger/{:triggername}/action/{:name}
func ActionDelete(w http.ResponseWriter, r *http.Request) {
	trigger := mux.Vars(r)["trigger"]
	t, err := base64.URLEncoding.DecodeString(trigger)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	tg := string(t)
	name := mux.Vars(r)["name"]
	q := &RedisQuery{
		Action:        "DEL",
		Options:       []interface{}{tg + ":" + name},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	<-q.resultChannel
	q = &RedisQuery{
		Action:        "SREM",
		Options:       []interface{}{tg + ":actions", tg + ":" + name},
		resultChannel: make(chan *QueryResult),
	}
	queryservice.queryChannel <- q
	queryresult := <-q.resultChannel
	if queryresult.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		w.Write([]byte("delete successful"))
	}
}
