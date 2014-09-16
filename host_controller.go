package main

import (
	"encoding/json"
	"github.com/fzzy/radix/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"sort"
	"strings"
)

// HostIndex GET /host
func (q *WebService) HostIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	client, err := redis.Dial(q.Network, q.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
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
	hosts, _ := client.Cmd("SMEMBERS", "hosts:"+user).List()
	var rst []interface{}
	for _, host := range hosts {
		query := make(map[string]interface{})
		query["name"] = host
		query["metric"] = "/api/v1/host/" + host
		rst = append(rst, query)
	}
	body, _ := json.Marshal(rst)
	w.Write(body)
}

// HostShow GET /host/{:name}
func (q *WebService) HostShow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, DELETE")
	host := strings.Replace(mux.Vars(r)["host"], "-", ".", -1)
	client, err := redis.Dial(q.Network, q.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer client.Close()
	user := loginFilter(r, client)
	if len(user) == 0 {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	reply := client.Cmd("SMEMBERS", "host:"+user+"_"+host)
	if reply.Err == nil {
		w.WriteHeader(http.StatusOK)
		query := make(map[string]interface{})
		query["name"] = host
		query["metrics_url"] = "/api/v1/host/" + host + "/metric"
		body, _ := json.Marshal(query)
		w.Write(body)
	} else {
		w.WriteHeader(http.StatusNotFound)
		log.Println("failed to get set", err)
	}
}

// HostDelete DELETE /host/{:name}
func (q *WebService) HostDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, DELETE")
	host := strings.Replace(mux.Vars(r)["host"], "-", ".", -1)
	client, err := redis.Dial(q.Network, q.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
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
	metricList, err := client.Cmd("SMEMBERS", "host:"+user+"_"+host).List()
	if err == nil {
		for _, v := range metricList {
			client.Append("DEL", v)
			client.Append("DEL", "archive:"+v)
			client.GetReply()
			reply := client.GetReply()
			if reply.Err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		reply := client.Cmd("DEL", "host:"+host)
		if reply.Err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

// HostMetricIndex GET /host/{:hostname}/metric
func (q *WebService) HostMetricIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	host := strings.Replace(mux.Vars(r)["host"], "-", ".", -1)
	client, err := redis.Dial(q.Network, q.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
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
	metricList, err := client.Cmd("SMEMBERS", "host:"+user+"_"+host).List()
	size := len(user)
	if err == nil {
		var rst []interface{}
		sort.Strings(metricList)
		for _, v := range metricList {
			reply := client.Cmd("HGET", v, "ttl")
			if reply.Err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			ttl, err := reply.Int()
			tp, err := client.Cmd("HGET", v, "type").List()
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			metric := make(map[string]interface{})
			metric["type"] = tp
			metric["ttl"] = ttl
			v = v[size+1:]
			metric["name"] = v
			metric["url"] = "/api/v1/metric/" + v
			rst = append(rst, metric)
		}
		if body, err := json.Marshal(rst); err == nil {
			w.Write(body)
		} else {
			w.WriteHeader(http.StatusInternalServerError)
		}
	} else {
		w.WriteHeader(http.StatusNotFound)
	}
}

// HostMetricDelete DELETE /host/{:hostname}/metric/{:name}
func (q *WebService) HostMetricDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
	host := strings.Replace(mux.Vars(r)["host"], "-", ".", -1)
	metric := mux.Vars(r)["name"]
	client, err := redis.Dial(q.Network, q.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
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
	client.Append("SREM", "host:"+user+"_"+host, metric)
	client.Append("DEL", "archive:"+user+"_"+metric)
	client.Append("DEL", user+"_"+metric)
	client.GetReply()
	client.GetReply()
	reply := client.GetReply()
	if reply.Err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
