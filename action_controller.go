package main

import (
	"encoding/base64"
	"encoding/json"
	"github.com/fzzy/radix/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

// ActionIndex GET /trigger/{:triggername}/action
func (q *WebService) ActionIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	trigger := mux.Vars(r)["trigger"]
	t, err := base64.URLEncoding.DecodeString(trigger)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	tg := string(t)
	client, err := redis.Dial(q.Network, q.RedisServer)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer client.Close()
	user := loginFilter(r, client)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	reply := client.Cmd("HGET", tg, "owner")
	if reply.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	owner, err := reply.Str()
	if user != owner {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	value, err := client.Cmd("SMEMBERS", tg+":actions").Str()
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
	var action NotifyAction
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
	client, err := redis.Dial(q.Network, q.RedisServer)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer client.Close()
	user := loginFilter(r, client)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	reply := client.Cmd("HGET", tg, "owner")
	if reply.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	owner, err := reply.Str()
	if user != owner {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	if _, err := client.Cmd("HGET", tg, "role").Str(); err != nil {
		w.WriteHeader(http.StatusNotFound)
		return
	}
	name := base64.URLEncoding.EncodeToString([]byte(action.Uri))
	client.Append("HMSET", tg+":"+name, "repeat", action.Repeat, "uri", action.Uri)
	client.Append("SADD", tg+":actions", tg+":"+name)
	client.GetReply()
	reply = client.GetReply()
	if reply.Err != nil {
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
func (q *WebService) ActionDelete(w http.ResponseWriter, r *http.Request) {
	trigger := mux.Vars(r)["trigger"]
	t, err := base64.URLEncoding.DecodeString(trigger)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	tg := string(t)
	name := mux.Vars(r)["name"]
	client, err := redis.Dial(q.Network, q.RedisServer)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer client.Close()
	user := loginFilter(r, client)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	owner, err := client.Cmd("HGET", tg, "owner").Str()
	if user != owner {
		w.WriteHeader(http.StatusForbidden)
		return
	}
	client.Append("DEL", tg+":"+name)
	client.Append("SREM", tg+":actions", tg+":"+name)
	client.GetReply()
	reply := client.GetReply()
	if reply.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		w.Write([]byte("delete successful"))
	}
}
