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
)

// ActionIndex GET /trigger/{:triggername}/action
func ActionIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	tg := mux.Vars(r)["trigger"]
	var err error
	configCon := configService.Get()
	defer configCon.Close()
	dataCon := dataService.Get()
	defer dataCon.Close()
	data, err := configCon.Do("KEYS", "actions:"+tg+":*")
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
	tg := mux.Vars(r)["trigger"]
	configCon := configService.Get()
	defer configCon.Close()
	if _, err := redis.String(configCon.Do("HGET", "trigger:"+tg, "exp")); err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	h := sha1.New()
	h.Write([]byte(action.Uri))
	name := base64.URLEncoding.EncodeToString(h.Sum(nil))
	_, err := configCon.Do("HMSET", "actions:"+tg+":"+name,
		"repeat", action.Repeat, "uri", action.Uri)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed insert"))
	} else {
		t := make(map[string]string)
		t["trigger_name"] = tg
		t["action_name"] = name
		t["url"] = "/api/v1/trigger/" + tg + "/" + name
		if body, err := json.Marshal(t); err == nil {
			w.Write(body)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

// ActionDelete DELETE /trigger/{:triggername}/action/{:name}
func ActionDelete(w http.ResponseWriter, r *http.Request) {
	tg := mux.Vars(r)["trigger"]
	name := mux.Vars(r)["name"]
	configCon := configService.Get()
	defer configCon.Close()
	_, err := configCon.Do("DEL", "actions:"+tg+":"+name)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		w.Write([]byte("delete successful"))
	}
}
