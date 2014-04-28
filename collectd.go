package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"net/http"
	"regexp"
	"strings"
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
	if len(c.Type) > 0 && c.Type != c.Plugin {
		metricName += "." + c.Type
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

func getMetricRate(key string, value float64, timestamp int64, dataType string, con redis.Conn) (float64, error) {
	var nValue float64
	rst, err := redis.Values(con.Do("HMGET", key, "value", "timestamp"))
	if err != nil {
		return 0, err
	}
	var t int64
	var v float64
	_, err = redis.Scan(rst, &v, &t)
	if err != nil {
		return 0, redis.ErrNil
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
	auth := r.Header.Get("Authorization")
	idents := strings.Split(auth, " ")
	if len(idents) < 2 || idents[0] != "Basic" {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	userId, _ := base64.StdEncoding.DecodeString(idents[1])
	idents = strings.Split(string(userId), ":")
	if len(idents) != 2 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	user := idents[0]
	// passwd := idents[1]
	// Todo check against user/passwd
	var dataset []CollectdJSON
	defer r.Body.Close()
	err := json.NewDecoder(r.Body).Decode(&dataset)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	con := q.Get()
	defer con.Close()
	for _, c := range dataset {
		for i := range c.Values {
			key := user + "_" + c.GetMetricName(i)
			t := int64(c.Timestamp)
			nValue, _ := getMetricRate(key, c.Values[i], t, c.DataSetTypes[i], con)
			_, _, err = q.writer.Publish(q.MetricTopic, []byte(fmt.Sprintf("%s %.2f %d", key, nValue, t)))
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			con.Send("HMSET", key, "rate_value", nValue, "value", c.Values[i], "timestamp", t)
			con.Send("SADD", "host:"+c.Host, key)
			con.Flush()
			con.Receive()
			_, err = con.Receive()
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
		}
	}
}
