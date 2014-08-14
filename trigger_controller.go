package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"regexp"
	"strings"
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
	con := q.Pool.Get()
	defer con.Close()
	user := loginFilter(r, con)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	owner, err := redis.String(con.Do("HGET", name, "owner"))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
		return
	}
	if user == owner {
		var recordList []interface{}
		var data []string
		for i := start / 14400; i <= end/14400; i++ {
			values, err := redis.String(con.Do("GET", fmt.Sprintf("archive:%s:%d", user+"_"+name, i)))
			if err != nil {
				log.Println(err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			data = append(data, values)
		}
		metricData := ParseTimeSeries(data)
		record := make(map[string]interface{})
		tgname := base64.URLEncoding.EncodeToString([]byte(name))
		record["name"] = name
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
func (q *WebService) TriggerCreate(w http.ResponseWriter, r *http.Request) {
	var tg Trigger
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
	con := q.Pool.Get()
	defer con.Close()
	user := loginFilter(r, con)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	_, err := redis.String(con.Do("HGET", tg.Name, "role"))
	if err == nil {
		w.WriteHeader(http.StatusNotAcceptable)
		w.Write([]byte(tg.Name + " exists"))
		return
	}
	con.Send("HMSET", tg.Name, "is_e", tg.IsExpression, "role", tg.Role, "owner", user)
	con.Send("SADD", "triggers", tg.Name)
	con.Flush()
	con.Receive()
	_, err = con.Receive()
	if err != nil {
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
	con := q.Pool.Get()
	defer con.Close()
	user := loginFilter(r, con)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	owner, _ := redis.String(con.Do("HGET", name, "owner"))
	isExpression, err := redis.Bool(con.Do("HGET", name, "is_e"))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	if owner != user {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if isExpression {
		con.Send("DEL", "archive:"+name)
		con.Send("DEL", name)
		con.Flush()
		con.Receive()
		_, err = con.Receive()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	} else {
		con.Do("HDEL", name, "is_e", "role")
	}
	keys, err := redis.Strings(con.Do("SMEMBERS", name+":actions"))
	for _, v := range keys {
		_, err = con.Do("DEL", v)
		if err != nil {
			break
		}
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to delete trigger"))
		return
	}
	_, err = con.Do("SREM", "triggers", name)
	if err != nil {
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
	con := q.Pool.Get()
	user := loginFilter(r, con)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	owner, _ := redis.String(con.Do("HGET", name, "owner"))
	if owner != user {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	raw_trigger_history, err := redis.Bytes(con.Do("GET", "trigger_history:"+name))
	var trigger_history []KeyValue
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
