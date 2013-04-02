package main

import (
	metrictools "../"
	"encoding/json"
	"github.com/gorilla/mux"
	"io/ioutil"
	"labix.org/v2/mgo/bson"
	"log"
	"net/http"
)

func TriggerActionIndexHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	tg := mux.Vars(r)["trigger"]
	var err error
	session := db_session.Clone()
	defer session.Close()
	var actions []metrictools.NotifyAction
	err = session.DB(dbname).C(notify_collection).
		Find(bson.M{"n": tg}).All(&actions)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		body, _ := json.Marshal(actions)
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}
}

func TriggerActionNewHandler(w http.ResponseWriter, r *http.Request) {
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
	session := db_session.Clone()
	defer session.Close()
	action.Name = mux.Vars(r)["trigger"]
	err = session.DB(dbname).C(notify_collection).Insert(action)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed insert"))
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Add successful"))
	}
}
func TriggerActionUpdateHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
		log.Println(err)
		return
	}
	tg_name := mux.Vars(r)["trigger"]
	name := mux.Vars(r)["name"]
	var action metrictools.NotifyAction
	if tg_name != name {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("json error"))
		return
	}
	if err = json.Unmarshal(body, &action); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("json error"))
		return
	} else {
		w.WriteHeader(http.StatusOK)
	}
	session := db_session.Clone()
	defer session.Close()
	action.Name = tg_name
	err = session.DB(dbname).C(notify_collection).
		Update(bson.M{"n": tg_name}, action)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("update successful"))
	}
}
func TriggerActionRemoveHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	name := mux.Vars(r)["name"]
	session := db_session.Clone()
	defer session.Close()
	err = session.DB(dbname).C(notify_collection).
		Remove(bson.M{"n": name})
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
		db_session.Refresh()
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("delete successful"))
	}
}
