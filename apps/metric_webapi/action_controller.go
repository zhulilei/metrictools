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
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST")
	name := mux.Vars(r)["trigger"]
	triggerName, err := base64.URLEncoding.DecodeString(name)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	triggerKey := string(metrictools.XorBytes([]byte(user), []byte(triggerName)))
	value, err := q.engine.GetSet("actions:" + string(triggerKey))
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
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST")
	tname := mux.Vars(r)["trigger"]
	triggerName, err := base64.URLEncoding.DecodeString(tname)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	triggerKey := string(metrictools.XorBytes([]byte(user), []byte(triggerName)))
	trigger, err := q.engine.GetTrigger(string(triggerKey))
	action.Name = string(metrictools.XorBytes([]byte(user), []byte(action.Uri)))
	err = q.engine.SaveNotifyAction(action)
	if err == nil {
		err = q.engine.SetAdd("actions:"+string(triggerKey), action.Name)
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed insert"))
	} else {
		t := make(map[string]string)
		aname := base64.URLEncoding.EncodeToString([]byte(action.Name))
		t["trigger_name"] = string(metrictools.XorBytes([]byte(user), []byte(trigger.Name)))
		t["action"] = action.Uri
		t["url"] = "/api/v1/trigger/" + tname + "/action/" + aname
		if body, err := json.Marshal(t); err == nil {
			w.Write(body)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

// ActionDelete DELETE /trigger/{:triggername}/action/{:name}
func (q *WebService) ActionDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Methods", "GET,DELETE")
	name := mux.Vars(r)["trigger"]
	triggerName, err := base64.URLEncoding.DecodeString(name)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	aname := mux.Vars(r)["name"]
	actionName, err := base64.URLEncoding.DecodeString(aname)
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	triggerKey := string(metrictools.XorBytes([]byte(user), []byte(triggerName)))
	trigger, err := q.engine.GetTrigger(string(triggerKey))
	if user != trigger.Owner {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	actionKey := string(metrictools.XorBytes([]byte(user), []byte(actionName)))
	if err == nil {
		err = q.engine.SetDelete("actions:"+string(triggerKey), actionKey)
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		w.Write([]byte("delete successful"))
	}
}
