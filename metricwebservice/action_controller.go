package main

import (
	metrictools "../"
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
	dataCon := dataService.Get()
	defer dataCon.Close()
	data, err := dataCon.Do("SMEMBERS", tg+":actions")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		body, _ := json.Marshal(data)
		w.Write(body)
	}
}

// ActionCreate POST /trigger/{:triggername}/action
func ActionCreate(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var action metrictools.NotifyAction
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
	dataCon := dataService.Get()
	defer dataCon.Close()
	if _, err := redis.String(dataCon.Do("HGET", tg, "role")); err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	name := base64.URLEncoding.EncodeToString([]byte(action.Uri))
	_, err = dataCon.Do("HMSET", tg+":"+name,
		"repeat", action.Repeat, "uri", action.Uri)
	_, err = dataCon.Do("SADD", tg+":actions", tg+":"+name)
	if err != nil {
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
	dataCon := dataService.Get()
	defer dataCon.Close()
	dataCon.Do("DEL", tg+":"+name)
	_, err = dataCon.Do("SREM", tg+":actions", tg+":"+name)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		w.Write([]byte("delete successful"))
	}
}
