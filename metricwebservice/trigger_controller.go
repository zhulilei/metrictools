package main

import (
	metrictools "../"
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

func TriggerShow(w http.ResponseWriter, r *http.Request) {
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

func TriggerCreate(w http.ResponseWriter, r *http.Request) {
	var tg metrictools.Trigger
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&tg); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	config_con := configservice.Get()
	defer config_con.Close()
	_, err := redis.String(config_con.Do("HGET", "trigger:"+tg.Name, "exp"))
	if err == nil {
		w.WriteHeader(http.StatusNotAcceptable)
		w.Write([]byte(tg.Name + " exists"))
		return
	}
	_, err = config_con.Do("HMSET", "trigger:"+tg.Name,
		"exp", tg.Expression,
		"relation", tg.Relation,
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

func TriggerDelete(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	config_con := configservice.Get()
	defer config_con.Close()
	data_con := dataservice.Get()
	defer data_con.Close()
	_, err := config_con.Do("DEL", "archive:"+name)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	_, err = config_con.Do("DEL", "trigger:"+name)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to delete trigger"))
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("delete successful"))
	}
}
