package main

import (
	metrictools "../"
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"sort"
	"strings"
)

func HostIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	data_con := dataservice.Get()
	defer data_con.Close()
	hosts, _ := redis.Strings(data_con.Do("KEYS", "host:*"))
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

func HostShow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, DELETE")
	host := strings.Replace(mux.Vars(r)["host"], "-", ".", -1)
	data_con := dataservice.Get()
	defer data_con.Close()
	_, err := redis.Strings(data_con.Do("SMEMBERS", "host:"+host))
	if err == nil {
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

func HostDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, DELETE")
	host := strings.Replace(mux.Vars(r)["host"], "-", ".", -1)
	data_con := dataservice.Get()
	defer data_con.Close()
	metric_list, err := redis.Strings(data_con.Do("SMEMBERS", "host:"+host))
	if err == nil {
		for _, v := range metric_list {
			_, err = data_con.Do("DEL", v)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			_, err = data_con.Do("DEL", "archive:"+v)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
		_, err = data_con.Do("DEL", "host:"+host)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
	}
	w.WriteHeader(http.StatusOK)
}

func HostMetricIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	host := strings.Replace(mux.Vars(r)["host"], "-", ".", -1)
	data_con := dataservice.Get()
	defer data_con.Close()
	metric_list, err := redis.Strings(data_con.Do("SMEMBERS", "host:"+host))
	if err == nil {
		var rst []interface{}
		sort.Strings(metric_list)
		for _, v := range metric_list {
			m, err := redis.Values(data_con.Do("HMGET", v, "dstype", "dsname", "interval", "host", "plugin", "plugin_instance", "type", "type_instance", "ttl"))
			if err != nil {
				log.Println("failed to hgetall", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			var item metrictools.MetricData
			_, err = redis.Scan(m, &item.DataSetType, &item.DataSetName, &item.Interval, &item.Host, &item.Plugin, &item.PluginInstance, &item.Type, &item.TypeInstance, &item.TTL)
			if err != nil {
				log.Println("failed to scanstruct", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			metric := make(map[string]interface{})
			name := item.Host + "_" + item.GetMetricName()
			metric["name"] = name
			metric["host"] = item.Host
			metric["plugin"] = item.Plugin
			metric["plugin_instance"] = item.PluginInstance
			metric["type"] = item.Type
			metric["type_instance"] = item.TypeInstance
			metric["interval"] = item.Interval
			metric["dsname"] = item.DataSetName
			metric["dstype"] = item.DataSetType
			metric["ttl"] = item.TTL
			metric["url"] = "/api/v1/metric/" + name
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

func HostMetricDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "DELETE")
	host := strings.Replace(mux.Vars(r)["host"], "-", ".", -1)
	metric := mux.Vars(r)["name"]
	data_con := dataservice.Get()
	defer data_con.Close()
	_, err := data_con.Do("SREM", "host:"+host, metric)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	_, err = data_con.Do("DEL", "archive:"+metric)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	_, err = data_con.Do("DEL", metric)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}
