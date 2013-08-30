package main

import (
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
)

func HostHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.WriteHeader(http.StatusOK)
	host := mux.Vars(r)["name"]
	var query []string
	data_con := dataservice.Get()
	defer data_con.Close()
	metric_list, err := redis.Strings(data_con.Do("KEYS", "archive:"+host+"*"))
	if err == nil {
		for _, v := range metric_list {
			query = append(query, v)
		}
	} else {
		log.Println("failed to get set", err)
	}
	w.Write(json_host_metric(query, host))
}

func HostClearMetricHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	w.WriteHeader(http.StatusMovedPermanently)

	host := mux.Vars(r)["name"]
	config_con := configservice.Get()
	defer config_con.Close()
	metric_list, err := redis.Strings(config_con.Do("SMEMBERS", host))
	if err == nil {
		data_con := dataservice.Get()
		defer data_con.Close()
		for _, v := range metric_list {
			data_con.Do("DEL", v)
			data_con.Do("DEL", "raw:"+v)
			data_con.Do("DEL", "archive:"+v)
			data_con.Do("DEL", "setting:"+v)
		}
	}
	_, err = config_con.Do("DEL", host)
	if err != nil {
		log.Println("failed to get set", err)
	}
	w.Header().Set("Location", r.Referer())
}

func HostListMetricHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.WriteHeader(http.StatusOK)
	host := mux.Vars(r)["host"]
	var query []string
	config_con := configservice.Get()
	defer config_con.Close()
	metric_list, err := redis.Strings(config_con.Do("SMEMBERS", host))
	if err == nil {
		for _, v := range metric_list {
			query = append(query, v)
		}
	} else {
		log.Println("failed to get set", err)
	}
	body, _ := json.Marshal(query)
	w.Write(body)
}

func HostDeleteMetricHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	w.WriteHeader(http.StatusOK)
	host := mux.Vars(r)["host"]
	metric := mux.Vars(r)["name"]
	config_con := configservice.Get()
	defer config_con.Close()
	data_con := dataservice.Get()
	defer data_con.Close()
	config_con.Do("SREM", host, metric)
	data_con.Do("DEL", "raw:"+metric)
	data_con.Do("DEL", "archive:"+metric)
	data_con.Do("DEL", metric)
	config_con.Do("DEL", "setting:"+metric)
}
