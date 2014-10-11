package main

import (
	"../.."
	"encoding/json"
	"fmt"
	"log"
	"net/http"
)

func (q *WebService) Collectd(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST")
	user := q.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	var dataset []metrictools.CollectdJSON
	defer r.Body.Close()
	err := json.NewDecoder(r.Body).Decode(&dataset)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	for _, c := range dataset {
		for i := range c.Values {
			key := user + "_" + c.GetMetricName(i)
			t := int64(c.Timestamp)
			oldt, oldv, err := metrictools.GetMetricValue(key, q.engine)
			var nValue float64
			if err == nil {
				nValue = c.GetMetricRate(oldv, oldt, i)
				err = q.producer.Publish(q.MetricTopic, []byte(fmt.Sprintf("%s %.2f %d", key, nValue, t)))
			}
			if err != nil {
				log.Println("collectd metric error", err)
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			q.engine.Do("raw", "HMSET", key, "rate_value", nValue, "value", c.Values[i], "timestamp", t)
		}
	}
}
