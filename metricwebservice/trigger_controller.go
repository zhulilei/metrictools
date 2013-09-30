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
	"strings"
)

func TriggerShow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	name := mux.Vars(r)["name"]
	starttime := r.FormValue("starttime")
	endtime := r.FormValue("endtime")
	start := gettime(starttime)
	end := gettime(endtime)
	if !checktime(start, end) {
		start = end - 3600*3
	}
	config_con := configservice.Get()
	defer config_con.Close()
	exp, err := redis.String(config_con.Do("HGET", "trigger:"+name, "exp"))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Find Failed"))
	} else {
		var record_list []interface{}
		data_con := dataservice.Get()
		defer data_con.Close()
		metric_data, err := redis.Strings(data_con.Do("ZRANGEBYSCORE", "archive:"+name, start, end))
		if err != nil {
			log.Println(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		record := make(map[string]interface{})
		record["name"] = exp
		record["values"] = metrictools.GenerateTimeseries(metric_data)
		record_list = append(record_list, record)
		rst := make(map[string]interface{})
		rst["metrics"] = record_list
		rst["url"] = "/api/v1/trigger/" + name
		if body, err := json.Marshal(rst); err == nil {
			w.Write(body)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

func TriggerCreate(w http.ResponseWriter, r *http.Request) {
	var tg metrictools.Trigger
	defer r.Body.Close()
	if err := json.NewDecoder(r.Body).Decode(&tg); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		log.Println(err)
		return
	}
	tg.Expression = strings.Trim(tg.Expression, " ")
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	h := sha1.New()
	h.Write([]byte(tg.Expression))
	tg.Name = base64.URLEncoding.EncodeToString(h.Sum(nil))
	config_con := configservice.Get()
	defer config_con.Close()
	_, err := redis.String(config_con.Do("HGET", "trigger:"+tg.Name, "exp"))
	if err == nil {
		w.WriteHeader(http.StatusNotAcceptable)
		w.Write([]byte(tg.Name + " exists"))
		return
	}
	_, err = config_con.Do("HMSET", "trigger:"+tg.Name,
		"exp", tg.Expression,
		"role", tg.Role)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed insert"))
	} else {
		t := make(map[string]string)
		t["name"] = tg.Name
		t["url"] = "/api/v1/trigger/" + tg.Name
		if body, err := json.Marshal(t); err == nil {
			w.Write(body)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	}
}

func TriggerDelete(w http.ResponseWriter, r *http.Request) {
	name := mux.Vars(r)["name"]
	config_con := configservice.Get()
	defer config_con.Close()
	data_con := dataservice.Get()
	defer data_con.Close()
	_, err := data_con.Do("DEL", "archive:"+name)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	_, err = config_con.Do("DEL", "trigger:"+name)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to delete trigger"))
		return
	}
	keys, err := redis.Strings(config_con.Do("KEYS", "actions:"+name+":*"))
	for _, v := range keys {
		if _, err = config_con.Do("DEL", v); err != nil {
			break
		}
	}
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Failed to delete trigger"))
	} else {
		w.Write([]byte("delete successful"))
	}
}
