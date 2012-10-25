package main

import (
	"encoding/json"
	"github.com/datastream/metrictools"
	"io/ioutil"
	"labix.org/v2/mgo/bson"
	"log"
	"net/http"
)

func alarm_controller(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		alarm_show(w, req)
	case "POST":
		alarm_save(w, req)
	case "PUT":
		alarm_update(w, req)
	case "DELETE":
		alarm_delete(w, req)
	}
}
func alarm_show(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	exp := req.FormValue("alarm_exp")
	var err error
	if len(exp) > 0 {
		session := db_session.Clone()
		defer session.Close()
		var query metrictools.Alarm
		err = session.DB(dbname).C("alarm").Find(bson.M{"exp": exp}).One(&query)
		var query2 []metrictools.AlarmAction
		err = session.DB(dbname).C("alarm_action").Find(bson.M{"exp": exp}).One(&query2)
		alm_info := &AlarmRequest{
			Alarm_info:    query,
			Alarm_actions: query2,
		}
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte("Find Failed"))
			db_session.Refresh()
		} else {
			body, _ := json.Marshal(alm_info)
			w.WriteHeader(http.StatusOK)
			w.Write(body)
		}
	} else {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Bad request"))
	}
}

func alarm_save(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Println(err)
	}
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	var alm_req AlarmRequest
	if err = json.Unmarshal(body, &alm_req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Deny"))
		return
	} else {
		w.WriteHeader(http.StatusOK)
	}
	session := db_session.Clone()
	defer session.Close()
	err = session.DB(dbname).C("alarm").Insert(alm_req.Alarm_info)
	for i := range alm_req.Alarm_actions {
		alm_req.Alarm_actions[i].Exp = alm_req.Alarm_info.Exp
		err = session.DB(dbname).C("alarm_action").Insert(alm_req.Alarm_actions[i])
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed insert"))
	} else {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("Add successful"))
	}
}
func alarm_update(w http.ResponseWriter, req *http.Request) {
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
			alm_type := req.FormValue("alarm_type")
			var alm_action metrictools.AlarmAction
			if err = json.Unmarshal(body, &alm_action); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("Deny"))
				return
			}
			err = session.DB(dbname).C("alarm_action").Update(bson.M{"exp": exp, "nm": name, "t": alm_type}, alm_action)
		} else {
			var alm_info metrictools.Alarm
			if err = json.Unmarshal(body, &alm_info); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				w.Write([]byte("Deny"))
				return
			}
			err = session.DB(dbname).C("alarm").Update(bson.M{"exp": exp}, alm_info)
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
func alarm_delete(w http.ResponseWriter, req *http.Request) {
	exp := req.FormValue("alarm_exp")
	alarm_type := req.FormValue("type")
	var err error
	if len(exp) > 0 {
		session := db_session.Clone()
		defer session.Close()
		if len(alarm_type) > 0 {
			name := req.FormValue("name")
			alm_type := req.FormValue("alarm_type")
			err = session.DB(dbname).C("alarm_action").Remove(bson.M{"exp": exp, "nm": name, "t": alm_type})
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
