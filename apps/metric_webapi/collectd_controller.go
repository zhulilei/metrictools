package main

import (
	"encoding/json"
	"fmt"
	"github.com/fzzy/radix/redis"
	"log"
	"net/http"
	"../.."
)

func (q *WebService) Collectd(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST")
	client, err := redis.Dial(q.Network, q.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer client.Close()
	user := basicAuth(r, client)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	var dataset []metrictools.CollectdJSON
	defer r.Body.Close()
	err = json.NewDecoder(r.Body).Decode(&dataset)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	for _, c := range dataset {
		for i := range c.Values {
			key := user + "_" + c.GetMetricName(i)
			t := int64(c.Timestamp)
			nValue, err := metrictools.GetMetricRate(key, c.Values[i], t, c.DataSetTypes[i], client)
			if err != nil {
				log.Println("get MetricRate failed", err)
			}
			err = q.producer.Publish(q.MetricTopic, []byte(fmt.Sprintf("%s %.2f %d", key, nValue, t)))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			client.Append("HMSET", key, "rate_value", nValue, "value", c.Values[i], "timestamp", t, "type", c.Type)
			client.Append("SADD", "host:"+user+"_"+c.Host, key)
			client.GetReply()
			reply := client.GetReply()
			if reply.Err != nil {
				log.Println("redis get reply err", reply.Err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
	}
}
