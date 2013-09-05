package main

import (
	metrictools "../"
	"encoding/json"
	"sort"
	"strings"
)

func gen_json(m map[string][]interface{}) []byte {
	var keys []string
	for k, _ := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var msg_list []interface{}
	for _, v := range keys {
		msg := map[string]interface{}{
			"key":    v,
			"values": m[v],
		}
		msg_list = append(msg_list, interface{}(msg))
	}
	var rst []byte
	if body, err := json.Marshal(msg_list); err == nil {
		rst = body
	}
	return rst
}

func json_metrics_value(m []metrictools.Record) []byte {
	metrics := make(map[string][]interface{})
	for _, v := range m {
		metrics[v.Key] = append(metrics[v.Key],
			[]interface{}{v.Timestamp, v.Value})
	}
	var keys []string
	for k, _ := range metrics {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var msg_list []interface{}
	for _, v := range keys {
		msg := map[string]interface{}{
			"key":    v,
			"values": metrics[v],
		}
		msg_list = append(msg_list, interface{}(msg))
	}
	var rst []byte
	if body, err := json.Marshal(msg_list); err == nil {
		rst = body
	}
	return rst
}

func get_pluginname(metric string) string {
	splitname := strings.Split(metric, "_")
	splitname = strings.Split(splitname[2], ".")
	return splitname[0]
}
