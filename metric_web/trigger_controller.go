package main

import (
	metrictools "../"
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
)

func TriggerShowHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	name := mux.Vars(r)["name"]
	config_con := configservice.Get()
	defer config_con.Close()
	data, err := redis.String(config_con.Do("GET", "trigger:"+name))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(data))
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
	if tg.EValue > tg.WValue && tg.Relation == metrictools.LESS {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("bad error/warning setting"))
		return
	}
	if tg.EValue < tg.WValue && tg.Relation == metrictools.GREATER {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("bad error/warning setting"))
		return
	}
	config_con := configservice.Get()
	defer config_con.Close()
	_, err = config_con.Do("HMSET", "trigger:"+tg.Name,
		"exp", tg.Expression,
		"persist", tg.Persist,
		"relation", tg.Relation,
		"warning", tg.WValue,
		"error", tg.EValue,
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
	config_con := configservice.Get()
	defer config_con.Close()
	_, err := config_con.Do("DEL", "trigger:"+name)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to delete trigger"))
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("delete successful"))
	}
}
