package main

import (
	"../.."
	"encoding/base64"
	"encoding/json"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

// ActionIndex GET /trigger/{:trigger}/action
func (q *WebService) ActionIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	name := mux.Vars(r)["trigger"]
	t, err := base64.URLEncoding.DecodeString(name)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	tg := string(t)
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	trigger, err := q.engine.GetTrigger(tg)
	if user != trigger.Owner {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	value, err := q.engine.GetSet("actions:" + tg)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		body, _ := json.Marshal(value)
		w.Write(body)
	}
}

// ActionCreate POST /trigger/{:triggername}/action
func (q *WebService) ActionCreate(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var action metrictools.NotifyAction
	if err := json.NewDecoder(r.Body).Decode(&action); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	tname := mux.Vars(r)["trigger"]
	t, err := base64.URLEncoding.DecodeString(tname)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	tg := string(t)
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	trigger, err := q.engine.GetTrigger(tg)
	if user != trigger.Owner {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	action.Name = base64.URLEncoding.EncodeToString([]byte(action.Uri))
	err = q.engine.SaveNotifyAction(action)
	if err == nil {
		err = q.engine.SetAdd("actions:"+tg, action.Name)
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed insert"))
	} else {
		t := make(map[string]string)
		t["trigger_name"] = tg
		t["action_name"] = action.Name
		t["url"] = "/api/v1/trigger/" + tname + "/" + action.Name
		if body, err := json.Marshal(t); err == nil {
			w.Write(body)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

// ActionDelete DELETE /trigger/{:triggername}/action/{:name}
func (q *WebService) ActionDelete(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["trigger"]
	t, err := base64.URLEncoding.DecodeString(name)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	tg := string(t)
	name = mux.Vars(r)["name"]
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	trigger, err := q.engine.GetTrigger(tg)
	if user != trigger.Owner {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if err == nil {
		err = q.engine.SetDelete("actions:"+tg, name)
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		w.Write([]byte("delete successful"))
	}
}
