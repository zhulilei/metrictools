package main

import (
	metrictools "../"
	"encoding/json"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
)

func TriggerShowHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	name := mux.Vars(r)["name"]
	data, err := wb.configservice.Do("GET", "trigger:"+name, nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		body, _ := json.Marshal(data.([]byte))
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}
}

func TriggerNewHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to read request"))
		log.Println(err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	var tg metrictools.Trigger
	if err = json.Unmarshal(body, &tg); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("failed to parse json"))
		return
	} else {
		w.WriteHeader(http.StatusOK)
	}
	redis_con := wb.config_redis_pool.Get()
	_, err = redis_con.Do("HMSET", "trigger:"+tg.Name,
		"exp", tg.Expression,
		"type", tg.TriggerType,
		"relation", tg.Relation,
		"values", tg.Values,
		"interval", tg.Interval,
		"role", tg.Role,
		"period", tg.Period,
		"stat", tg.Stat)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed insert"))
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Add successful"))
	}
}

func TriggerRemoveHandler(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	_, err := wb.configservice.Do("DEL", "trigger:"+name, nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to delete trigger"))
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("delete successful"))
	}
}
