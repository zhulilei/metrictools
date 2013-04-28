package main

import (
	metrictools "../"
	"encoding/json"
	"github.com/gorilla/mux"
	"io/ioutil"
	"log"
	"net/http"
)

func ActionIndexHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	tg := mux.Vars(r)["t_name"]
	var err error
	data, err := wb.configservice.Do("KEYS", "actions:"+tg+"*", nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		body, _ := json.Marshal(data)
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}
}

func ActionNewHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
		log.Println(err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	var action metrictools.NotifyAction
	if err = json.Unmarshal(body, &action); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Json error"))
		return
	} else {
		w.WriteHeader(http.StatusOK)
	}
	tg := mux.Vars(r)["t_name"]
	redis_con := wb.config_redis_pool.Get()
	_, err = redis_con.Do("HMSET", "actions:"+tg+":"+action.Name,
		"repeat", action.Repeat,
		"uri", action.Uri)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed insert"))
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Add successful"))
	}
}

func ActionRemoveHandler(w http.ResponseWriter, r *http.Request) {
	tg := mux.Vars(r)["t_name"]
	name := mux.Vars(r)["name"]
	_, err := wb.configservice.Do("DEL", "actions:"+tg+":"+name, nil)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("delete successful"))
	}
}
