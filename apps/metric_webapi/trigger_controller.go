package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/fzzy/radix/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"regexp"
	"strings"
	"../.."
)

// TriggerShow  GET /trigger/{:name}
func (q *WebService) TriggerShow(w http.ResponseWriter, r *http.Request) {
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
	client, err := redis.Dial(q.Network, q.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer client.Close()
	user := loginFilter(r, client)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	owner, err := client.Cmd("HGET", name, "owner").Str()
	if err != nil {
		log.Println("redis connection err", err)
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
		return
	}
	if user == owner {
		var recordList []interface{}
		var data []string
		for i := start / 14400; i <= end/14400; i++ {
			values, err := client.Cmd("GET", fmt.Sprintf("archive:%s:%d", user+"_"+name, i)).Str()
			if err != nil {
				log.Println(err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			data = append(data, values)
		}
		metricData := metrictools.ParseTimeSeries(data)
		record := make(map[string]interface{})
		tgname := base64.URLEncoding.EncodeToString([]byte(name))
		record["name"] = name
		record["values"] = metrictools.GenerateTimeseries(metricData)
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
func (q *WebService) TriggerCreate(w http.ResponseWriter, r *http.Request) {
	var tg metrictools.Trigger
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&tg); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	tg.Name = strings.Trim(tg.Name, " ")
	tgname := base64.URLEncoding.EncodeToString([]byte(tg.Name))
	tg.IsExpression, _ = regexp.MatchString(`(\+|-|\*|/)`, tg.Name)
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	client, err := redis.Dial(q.Network, q.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer client.Close()
	user := loginFilter(r, client)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	reply := client.Cmd("HGET", tg.Name, "role")
	if reply.Err == nil {
		w.WriteHeader(http.StatusNotAcceptable)
		w.Write([]byte(tg.Name + " exists"))
		return
	}
	client.Append("HMSET", tg.Name, "is_e", tg.IsExpression, "role", tg.Role, "owner", user)
	client.Append("SADD", "triggers", tg.Name)
	client.GetReply()
	reply = client.GetReply()
	if reply.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed insert"))
	} else {
		t := make(map[string]string)
		t["name"] = tgname
		t["url"] = "/api/v1/trigger/" + t["name"]
		if body, err := json.Marshal(t); err == nil {
			w.Write(body)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

// TriggerDelete DELETE /trigger/{:name}
func (q *WebService) TriggerDelete(w http.ResponseWriter, r *http.Request) {
	tg := mux.Vars(r)["name"]
	n, err := base64.URLEncoding.DecodeString(tg)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	name := string(n)
	client, err := redis.Dial(q.Network, q.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer client.Close()
	user := loginFilter(r, client)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	owner, _ := client.Cmd("HGET", name, "owner").Str()
	isExpression, err := client.Cmd("HGET", name, "is_e").Bool()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if owner != user {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if isExpression {
		client.Append("DEL", "archive:"+name)
		client.Append("DEL", name)
		client.GetReply()
		reply := client.GetReply()
		if reply.Err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	} else {
		client.Cmd("HDEL", name, "is_e", "role")
	}
	keys, err := client.Cmd("SMEMBERS", name+":actions").List()
	for _, v := range keys {
		reply := client.Cmd("DEL", v)
		if reply.Err != nil {
			err = reply.Err
			break
		}
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to delete trigger"))
		return
	}
	reply := client.Cmd("SREM", "triggers", name)
	if reply.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to delete trigger"))
	} else {
		w.Write([]byte("delete successful"))
	}
}

// TriggerHistoryShow /triggerhistory/#{name}
func (q *WebService) TriggerHistoryShow(w http.ResponseWriter, r *http.Request) {
	tg := mux.Vars(r)["name"]
	n, err := base64.URLEncoding.DecodeString(tg)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	name := string(n)
	client, err := redis.Dial(q.Network, q.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer client.Close()
	user := loginFilter(r, client)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	owner, _ := client.Cmd("HGET", name, "owner").Str()
	if owner != user {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	raw_trigger_history, err := client.Cmd("GET", "trigger_history:"+name).Bytes()
	var trigger_history []metrictools.KeyValue
	if err := json.Unmarshal(raw_trigger_history, &trigger_history); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed find trigger history"))
		return
	}
	var timeserires [][]interface{}
	for _, val := range trigger_history {
		timeserires = append(timeserires, []interface{}{val.GetTimestamp(), val.GetValue()})
	}
	var recordList []interface{}
	record := make(map[string]interface{})
	record["name"] = tg
	record["values"] = timeserires
	recordList = append(recordList, record)
	rst := make(map[string]interface{})
	rst["metrics"] = recordList
	rst["url"] = "/api/v1/triggerhistory/" + tg
	if body, err := json.Marshal(rst); err == nil {
		w.Write(body)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
