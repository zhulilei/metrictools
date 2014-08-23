package main

import (
	"encoding/json"
	"fmt"
	"github.com/fzzy/radix/redis"
	"net/http"
	"regexp"
	"strconv"
)

// CollectdJSON is collectd's json data format
type CollectdJSON struct {
	Values         []float64 `json:"values"`
	DataSetTypes   []string  `json:"dstypes"`
	DataSetNames   []string  `json:"dsnames"`
	Timestamp      float64   `json:"time"`
	Interval       float64   `json:"interval"`
	Host           string    `json:"host"`
	Plugin         string    `json:"plugin"`
	PluginInstance string    `json:"plugin_instance"`
	Type           string    `json:"type"`
	TypeInstance   string    `json:"type_instance"`
}

func (c *CollectdJSON) GetMetricName(index int) string {
	metricName := c.Host + "_" + c.Plugin
	if len(c.PluginInstance) > 0 {
		if matched, _ := regexp.MatchString(`^\d+$`, c.PluginInstance); matched {
			metricName += c.PluginInstance
		} else {
			metricName += "_" + c.PluginInstance
		}
	}
	tSize := len(c.Type)
	pSize := len(c.Plugin)
	if tSize > 0 {
		if pSize <= tSize && c.Type[:pSize] == c.Plugin {
			metricName += c.Type[pSize:]
		} else {
			metricName += "." + c.Type
		}
	}
	if len(c.TypeInstance) > 0 {
		if matched, _ := regexp.MatchString(`^\d+$`, c.TypeInstance); matched {
			metricName += c.TypeInstance
		} else {
			metricName += "_" + c.TypeInstance
		}
	}
	if c.DataSetNames[index] != "value" {
		metricName += "." + c.DataSetNames[index]
	}
	return metricName
}

func getMetricRate(key string, value float64, timestamp int64, dataType string, client *redis.Client) (float64, error) {
	var nValue float64
	rst, err := client.Cmd("HMGET", key, "value", "timestamp").List()
	if err != nil {
		return 0, err
	}
	var t int64
	var v float64
	t, _ = strconv.ParseInt(rst[0], 0, 64)
	v, err = strconv.ParseFloat(rst[1], 64)
	if err != nil {
		return 0, err
	}
	if dataType == "counter" || dataType == "derive" {
		nValue = (value - v) / float64(timestamp-t)
		if nValue < 0 {
			nValue = 0
		}
	} else {
		nValue = value
	}
	return nValue, err
}

func (q *WebService) Collectd(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "POST")
	client, err := q.Pool.Get()
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	defer q.Pool.Put(client)
	user := basicAuth(r, client)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	var dataset []CollectdJSON
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
			nValue, _ := getMetricRate(key, c.Values[i], t, c.DataSetTypes[i], client)
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
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
	}
}
