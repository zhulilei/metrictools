package main

import (
	metrictools "../"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

func ActionIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	tg := mux.Vars(r)["trigger"]
	var err error
	config_con := configservice.Get()
	defer config_con.Close()
	data_con := dataservice.Get()
	defer data_con.Close()
	data, err := config_con.Do("KEYS", "actions:"+tg+":*")
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		body, _ := json.Marshal(data)
		w.Write(body)
	}
}

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
	config_con := configservice.Get()
	defer config_con.Close()
	h := sha1.New()
	h.Write([]byte(action.Uri))
	name := base64.URLEncoding.EncodeToString(h.Sum(nil))
	_, err := config_con.Do("HMSET", "actions:"+tg+":"+name,
		"repeat", action.Repeat, "uri", action.Uri)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed insert"))
	} else {
		w.Write([]byte("Add successful"))
	}
}

func ActionDelete(w http.ResponseWriter, r *http.Request) {
	tg := mux.Vars(r)["trigger"]
	name := mux.Vars(r)["name"]
	config_con := configservice.Get()
	defer config_con.Close()
	_, err := config_con.Do("DEL", "actions:"+tg+":"+name)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		w.Write([]byte("delete successful"))
	}
}
