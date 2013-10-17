package main

import (
	metrictools "../"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"strings"
)

// TriggerShow  GET /trigger/{:name}
func TriggerShow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	name := mux.Vars(r)["name"]
	starttime := r.FormValue("starttime")
	endtime := r.FormValue("endtime")
	start := gettime(starttime)
	end := gettime(endtime)
	if !checktime(start, end) {
		start = end - 3600*3
	}
	configCon := configService.Get()
	defer configCon.Close()
	exp, err := redis.String(configCon.Do("HGET", "trigger:"+name, "exp"))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		var recordList []interface{}
		dataCon := dataService.Get()
		defer dataCon.Close()
		metricData, err := redis.Strings(dataCon.Do("ZRANGEBYSCORE", "archive:"+name, start, end))
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		record := make(map[string]interface{})
		record["name"] = exp
		record["values"] = metrictools.GenerateTimeseries(metricData)
		recordList = append(recordList, record)
		rst := make(map[string]interface{})
		rst["metrics"] = recordList
		rst["url"] = "/api/v1/trigger/" + name
		if body, err := json.Marshal(rst); err == nil {
			w.Write(body)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

// TriggerCreate POST /trigger
func TriggerCreate(w http.ResponseWriter, r *http.Request) {
	var tg metrictools.Trigger
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&tg); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	tg.Expression = strings.Trim(tg.Expression, " ")
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	h := sha1.New()
	h.Write([]byte(tg.Expression))
	tg.Name = base64.URLEncoding.EncodeToString(h.Sum(nil))
	configCon := configService.Get()
	defer configCon.Close()
	_, err := redis.String(configCon.Do("HGET", "trigger:"+tg.Name, "exp"))
	if err == nil {
		w.WriteHeader(http.StatusNotAcceptable)
		w.Write([]byte(tg.Name + " exists"))
		return
	}
	_, err = configCon.Do("HMSET", "trigger:"+tg.Name,
		"exp", tg.Expression,
		"role", tg.Role)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed insert"))
	} else {
		t := make(map[string]string)
		t["name"] = tg.Name
		t["url"] = "/api/v1/trigger/" + tg.Name
		if body, err := json.Marshal(t); err == nil {
			w.Write(body)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

// TriggerDelete DELETE /trigger/{:name}
func TriggerDelete(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	configCon := configService.Get()
	defer configCon.Close()
	dataCon := dataService.Get()
	defer dataCon.Close()
	_, err := dataCon.Do("DEL", "archive:"+name)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	_, err = configCon.Do("DEL", "trigger:"+name)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to delete trigger"))
		return
	}
	keys, err := redis.Strings(configCon.Do("KEYS", "actions:"+name+":*"))
	for _, v := range keys {
		if _, err = configCon.Do("DEL", v); err != nil {
			break
		}
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to delete trigger"))
	} else {
		w.Write([]byte("delete successful"))
	}
}
