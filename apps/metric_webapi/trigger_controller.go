package main

import (
	"../.."
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"strings"
)

// TriggerShow  GET /trigger/{:name}
func (q *WebService) TriggerShow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	tg := mux.Vars(r)["name"]
	starttime := r.FormValue("starttime")
	endtime := r.FormValue("endtime")
	start := gettime(starttime)
	end := gettime(endtime)
	if !checktime(start, end) {
		start = end - 3600*3
	}
	name, err := base64.URLEncoding.DecodeString(tg)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	triggerName := string(name)
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	triggerKey := string(metrictools.XorBytes([]byte(user), []byte(triggerName)))
	var recordList []interface{}
	var data []string
	for i := start / 14400; i <= end/14400; i++ {
		values, err := q.engine.GetValues(fmt.Sprintf("arc:%s:%d", triggerKey, i))
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		data = append(data, values...)
	}
	metricData := metrictools.ParseTimeSeries(data)
	record := make(map[string]interface{})
	record["name"] = triggerName
	record["values"] = metrictools.GenerateTimeseries(metricData)
	recordList = append(recordList, record)
	rst := make(map[string]interface{})
	rst["metrics"] = recordList
	if body, err := json.Marshal(rst); err == nil {
		w.Write(body)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}

// TriggerCreate POST /trigger
func (q *WebService) TriggerCreate(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Methods", "POST")
	var tg metrictools.Trigger
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&tg); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	tg.Name = strings.Trim(tg.Name, " ")
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	tg.Owner = user
	triggerKey := string(metrictools.XorBytes([]byte(user), []byte(tg.Name)))
	tg.Name = triggerKey
	err := q.engine.SaveTrigger(tg)
	q.engine.SetAdd("triggers", triggerKey)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed insert"))
	} else {
		t := make(map[string]string)
		t["name"] = string(metrictools.XorBytes([]byte(user), []byte(tg.Name)))
		tgname := base64.URLEncoding.EncodeToString([]byte(t["name"]))
		t["url"] = "/api/v1/trigger/" + tgname
		if body, err := json.Marshal(t); err == nil {
			w.Write(body)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

// TriggerDelete DELETE /trigger/{:name}
func (q *WebService) TriggerDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
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
	triggerKey := string(metrictools.XorBytes([]byte(user), []byte(name)))
	trigger, err := q.engine.GetTrigger(triggerKey)
	if trigger.Owner != user {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	keys, err := q.engine.GetSet("actions:" + triggerKey)
	var args []interface{}
	for _, v := range keys {
		args = append(args, v)
	}
	args = append(args, "trigger:"+triggerKey)
	err = q.engine.DeleteData(args...)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to delete trigger"))
		return
	}
	err = q.engine.SetDelete("triggers", triggerKey)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to delete trigger"))
	} else {
		w.Write([]byte("delete successful"))
	}
}

// TriggerHistoryShow /triggerhistory/#{name}
func (q *WebService) TriggerHistoryShow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Methods", "GET")
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
	triggerKey := string(metrictools.XorBytes([]byte(user), []byte(name)))
	reply, err := q.engine.GetValues("tgh:" + triggerKey)
	rawTriggerHistory := []byte(reply[0])
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
	record["name"] = name
	record["values"] = timeserires
	recordList = append(recordList, record)
	rst := make(map[string]interface{})
	rst["metrics"] = recordList
	if body, err := json.Marshal(rst); err == nil {
		w.Write(body)
	} else {
		w.WriteHeader(http.StatusInternalServerError)
	}
}
