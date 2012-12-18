package main

import (
	"encoding/json"
	"github.com/datastream/metrictools"
	"io/ioutil"
	"labix.org/v2/mgo/bson"
	"log"
	"net/http"
)

func trigger_controller(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		trigger_show(w, req)
	case "POST":
		trigger_save(w, req)
	case "PUT":
		trigger_update(w, req)
	case "DELETE":
		trigger_delete(w, req)
	}
}
func trigger_show(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	exp := req.FormValue("trigger")
	var err error
	if len(exp) > 0 {
		session := db_session.Clone()
		defer session.Close()
		var query metrictools.Trigger
		err = session.DB(dbname).C("Trigger").Find(bson.M{"exp": exp}).One(&query)
		var query2 []metrictools.AlarmAction
		err = session.DB(dbname).C("alarm_action").Find(bson.M{"exp": exp}).One(&query2)
		tg_info := &TriggerRequest{
			trigger: query,
			actions: query2,
		}
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Find Failed"))
			db_session.Refresh()
		} else {
			body, _ := json.Marshal(tg_info)
			w.WriteHeader(http.StatusOK)
			w.Write(body)
		}
	} else {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Bad request"))
	}
}

func trigger_save(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Println(err)
	}
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	var tg_req TriggerRequest
	if err = json.Unmarshal(body, &tg_req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Deny"))
		return
	} else {
		w.WriteHeader(http.StatusOK)
	}
	session := db_session.Clone()
	defer session.Close()
	err = session.DB(dbname).C("alarm").Insert(tg_req.trigger)
	for i := range tg_req.actions {
		tg_req.actions[i].Exp = tg_req.trigger.Exp
		err = session.DB(dbname).C("alarm_action").Insert(tg_req.actions[i])
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed insert"))
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Add successful"))
	}
}
func trigger_update(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Println(err)
	}
	exp := req.FormValue("alarm_exp")
	alarm_type := req.FormValue("type")
	if len(exp) > 0 {
		session := db_session.Clone()
		defer session.Close()
		if len(alarm_type) > 0 {
			name := req.FormValue("name")
			tg_type := req.FormValue("alarm_type")
			var tg_action metrictools.AlarmAction
			if err = json.Unmarshal(body, &tg_action); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("Deny"))
				return
			}
			err = session.DB(dbname).C("alarm_action").Update(bson.M{"exp": exp, "nm": name, "t": tg_type}, tg_action)
		} else {
			var tg_info metrictools.Trigger
			if err = json.Unmarshal(body, &tg_info); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("Deny"))
				return
			}
			err = session.DB(dbname).C("alarm").Update(bson.M{"exp": exp}, tg_info)
		}
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Find Failed"))
			db_session.Refresh()
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("delete successful"))
		}
	} else {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Bad request"))
	}
}
func trigger_delete(w http.ResponseWriter, req *http.Request) {
	exp := req.FormValue("alarm_exp")
	alarm_type := req.FormValue("type")
	var err error
	if len(exp) > 0 {
		session := db_session.Clone()
		defer session.Close()
		if len(alarm_type) > 0 {
			name := req.FormValue("name")
			tg_type := req.FormValue("alarm_type")
			err = session.DB(dbname).C("alarm_action").Remove(bson.M{"exp": exp, "nm": name, "t": tg_type})
		} else {
			err = session.DB(dbname).C("alarm_action").Remove(bson.M{"exp": exp})
			err = session.DB(dbname).C("alarm").Remove(bson.M{"exp": exp})
		}
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Find Failed"))
			db_session.Refresh()
		} else {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte("delete successful"))
		}
	} else {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Bad request"))
	}
}
