package main

import (
	metrictools "../"
	"encoding/json"
	"github.com/garyburd/redigo/redis"
	"github.com/gorilla/mux"
	"log"
	"net/http"
	"sort"
)

func HostIndex(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	data_con := dataservice.Get()
	defer data_con.Close()
	hosts, _ := redis.Strings(data_con.Do("KEYS", "*"))
	var rst []interface{}
	for _, host := range hosts {
		if host[:8] != "archive:" {
			query := make(map[string]interface{})
			query["name"] = host
			query["metric"] = "/host/" + host
			rst = append(rst, query)
		}
	}
	body, _ := json.Marshal(rst)
	w.Write(body)
}

func HostShow(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, DELETE")
	host := mux.Vars(r)["name"]
	data_con := dataservice.Get()
	defer data_con.Close()
	_, err := redis.Strings(data_con.Do("SMEMBERS", host))
	if err == nil {
		w.WriteHeader(http.StatusOK)
		query := make(map[string]interface{})
		query["name"] = host
		query["metric"] = "/host/" + host + "/metric"
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
	host := mux.Vars(r)["name"]
	data_con := dataservice.Get()
	defer data_con.Close()
	metric_list, err := redis.Strings(data_con.Do("SMEMBERS", host))
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
		_, err = data_con.Do("DEL", host)
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
	host := mux.Vars(r)["host"]
	data_con := dataservice.Get()
	defer data_con.Close()
	metric_list, err := redis.Strings(data_con.Do("SMEMBERS", host))
	if err == nil {
		var rst []interface{}
		sort.Strings(metric_list)
		for _, v := range metric_list {
			m, err := redis.Values(data_con.Do("HGETALL", v))
			if err != nil {
				log.Println("failed to hgetall", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			if m == nil {
				continue
			}
			var item metrictools.MetricData
			err = redis.ScanStruct(m, &item)
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
			rst = append(rst, name)
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
	host := mux.Vars(r)["host"]
	metric := mux.Vars(r)["name"]
	data_con := dataservice.Get()
	defer data_con.Close()
	_, err := data_con.Do("SREM", host, metric)
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
