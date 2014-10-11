package main

import (
	"../.."
	"encoding/base64"
	"encoding/json"
	"fmt"
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
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	reply, err := q.engine.Do("string", "HGET", name, "owner")
	owner := reply.(string)
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
			reply, err := q.engine.Do("string", "GET", fmt.Sprintf("archive:%s:%d", user+"_"+name, i))
			values := reply.(string)
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
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	_, err := q.engine.Do("string", "HGET", tg.Name, "role")
	if err == nil {
		w.WriteHeader(http.StatusNotAcceptable)
		w.Write([]byte(tg.Name + " exists"))
		return
	}
	q.engine.Do("string", "HMSET", tg.Name, "is_e", tg.IsExpression, "role", tg.Role, "owner", user)
	_, err = q.engine.Do("raw", "SADD", "triggers", tg.Name)
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
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	reply, _ := q.engine.Do("string", "HGET", name, "owner")
	owner := reply.(string)
	if owner != user {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	q.engine.Do("string", "DEL", name, "archive:"+name)
	reply, err = q.engine.Do("strings", "SMEMBERS", name+":actions")
	keys := reply.([]string)
	var args []interface{}
	for _, v := range keys {
		args = append(args, v)
	}
	_, err = q.engine.Do("raw", "DEL", args...)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to delete trigger"))
		return
	}
	_, err = q.engine.Do("raw", "SREM", "triggers", name)
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
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	reply, err := q.engine.Do("string", "HGET", name, "owner")
	owner := reply.(string)
	if owner != user {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	reply, err = q.engine.Do("string", "GET", "trigger_history:"+name)
	rawTriggerHistory := []byte(reply.(string))
	var triggerHistory []metrictools.KeyValue
	if err := json.Unmarshal(rawTriggerHistory, &triggerHistory); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed find trigger history"))
		return
	}
	var timeserires [][]interface{}
	for _, val := range triggerHistory {
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
