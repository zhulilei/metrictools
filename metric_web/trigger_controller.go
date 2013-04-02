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

func TriggerShowHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	name := mux.Vars(r)["name"]
	var err error
	session := db_session.Clone()
	defer session.Close()
	var tg metrictools.Trigger
	err = session.DB(dbname).C(trigger_collection).
		Find(bson.M{"n": name}).One(&tg)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		body, _ := json.Marshal(tg)
		w.WriteHeader(http.StatusOK)
		w.Write(body)
	}
}

func TriggerNewHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
		log.Println(err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	var tg metrictools.Trigger
	if err = json.Unmarshal(body, &tg); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Json error"))
		return
	} else {
		w.WriteHeader(http.StatusOK)
	}
	session := db_session.Clone()
	defer session.Close()
	err = session.DB(dbname).C(trigger_collection).Insert(tg)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed insert"))
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Add successful"))
	}
}
func TriggerUpdateHandler(w http.ResponseWriter, r *http.Request) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
		log.Println(err)
		return
	}
	name := mux.Vars(r)["name"]
	var tg metrictools.Trigger
	if err = json.Unmarshal(body, &tg); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("json error"))
		return
	} else {
		w.WriteHeader(http.StatusOK)
	}
	session := db_session.Clone()
	defer session.Close()
	err = session.DB(dbname).C(trigger_collection).
		Update(bson.M{"n": name}, tg)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("update successful"))
	}
}
func TriggerRemoveHandler(w http.ResponseWriter, r *http.Request) {
	var err error
	name := mux.Vars(r)["name"]
	session := db_session.Clone()
	defer session.Close()
	err = session.DB(dbname).C(notify_collection).
		Remove(bson.M{"n": name})
	err = session.DB(dbname).C(trigger_collection).
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
