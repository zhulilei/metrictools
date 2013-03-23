package main

import (
	metrictools "../"
	"encoding/json"
	"regexp"
	"sort"
	"strings"
)

func json_metrics_value(m []metrictools.Record) []byte {
	metrics := make(map[string][]interface{})
	for _, v := range m {
		metrics[v.K] = append(metrics[v.K],
			[]interface{}{v.T, v.V})
	}
	var keys []string
	for k, _ := range metrics {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var msg_list []interface{}
	for l := range keys {
		msg := map[string]interface{}{
			"key":    keys[l],
			"values": metrics[keys[l]],
		}
		msg_list = append(msg_list, interface{}(msg))
	}
	var rst []byte
	if body, err := json.Marshal(msg_list); err == nil {
		rst = body
	}
	return rst
}

func gen_value(values map[int64]float64, name string) string {
	var rst string
	var _values [][]interface{}
	for k, v := range values {
		_values = append(_values, []interface{}{k, v})
	}
	msg := map[string]interface{}{"key": name, "values": _values}
	if body, err := json.Marshal(msg); err != nil {
		rst = ""
	} else {
		rst = string(body)
	}
	return rst
}

func json_host_metric(metrics []string, host string) []byte {
	host_metric := make(map[string][]string)
	var rst []byte
	for _, v := range metrics {
		host_metric[get_pluginname(v)] = append(
			host_metric[get_pluginname(v)], v)
	}
	var keys []string
	for k, _ := range host_metric {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	host_msg := make(map[string]interface{})
	host_msg["key"] = host
	var val []interface{}
	for _, key := range keys {
		sort.Strings(host_metric[key])
		var c []interface{}
		switch key {
		case "cpu":
			{
				c = gen_cpu(host_metric[key], host)
			}
		case "partion":
			{
				c = gen_partion(host_metric[key], host)
			}
		case "apache":
			{
				c = gen_apache(host_metric[key], host)
			}
		case "disk":
			{
				c = gen_disk(host_metric[key], host)
			}
		case "interface":
			{
				c = gen_interface(host_metric[key], host)
			}
		default:
			{
				var metrics string
				for i := range host_metric[key] {
					metrics += host_metric[key][i] + ","
				}
				msg := map[string]interface{}{
					"key": key,
					"url": "/monitor?metricsname=" +
						metrics[:len(metrics)-1] +
						"&host=" + host +
						"&type=" + key}
				val = append(val, msg)
			}
		}
		if len(c) > 0 {
			msg := map[string]interface{}{
				"key": key, "_values": c}
			val = append(val, msg)
		}
	}
	host_msg["values"] = val
	if body, err := json.Marshal(host_msg); err == nil {
		rst = body
	}
	return rst
}

func gen_cpu(v []string, host string) []interface{} {
	cpus := make(map[string][]string)
	for i := range v {
		reg, _ := regexp.Compile("cpu_[0-9]{1,2}")
		st := reg.FindString(v[i])
		st_list := strings.Split(st, "_")
		st = st_list[0] + st_list[1]
		cpus[st] = append(cpus[st], v[i])
	}
	return sort_json(cpus, host, "cpu")
}
func gen_partion(v []string, host string) []interface{} {
	partion := make(map[string][]string)
	for i := range v {
		reg, _ := regexp.Compile(
			"partion.*(free|used|used_percent|total)")
		st := reg.FindString(v[i])
		st_list := strings.Split(st, ".")
		st = "/"
		for i := 1; i < len(st_list); i++ {
			if st_list[i] == "free" || st_list[i] == "used" {
				continue
			}
			if st_list[i] == "total" {
				st += "df/"
				continue
			}
			st += st_list[i] + "/"
		}
		st = st[:len(st)-1]
		partion[st] = append(partion[st], v[i])
	}
	return sort_json(partion, host, "partion")
}
func gen_apache(v []string, host string) []interface{} {
	apache := make(map[string][]string)
	for i := range v {
		reg, _ := regexp.Compile(
			"apache_(scoreboard|connection|byte|request|idle)")
		st := reg.FindString(v[i])
		apache[st] = append(apache[st], v[i])
	}
	return sort_json(apache, host, "apache")
}
func gen_disk(v []string, host string) []interface{} {
	disk := make(map[string][]string)
	for i := range v {
		reg, _ := regexp.Compile("sd[a-z]{1,2}")
		st := reg.FindString(v[i])
		disk[st] = append(disk[st], v[i])
	}
	return sort_json(disk, host, "diskio")
}
func gen_interface(v []string, host string) []interface{} {
	eths := make(map[string][]string)
	for i := range v {
		reg, _ := regexp.Compile(
			"((eth|bond|br)[0-9]{1,2}|lo).(tx|rx|if)")
		st := reg.FindString(v[i])
		st_list := strings.Split(st, ".")
		st = st_list[0]
		eths[st] = append(eths[st], v[i])
	}
	return sort_json(eths, host, "network")
}
func sort_json(arrary map[string][]string, host string, data_type string) []interface{} {
	var rst []interface{}
	var keys []string
	for k, _ := range arrary {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for l := range keys {
		var metrics string
		sort.Strings(arrary[keys[l]])
		for i := range arrary[keys[l]] {
			metrics += arrary[keys[l]][i] + ","
		}
		msg := map[string]interface{}{
			"key": keys[l],
			"url": "/monitor?metricsname=" +
				metrics[:len(metrics)-1] +
				"&host=" + host + "&type=" + data_type}
		rst = append(rst, msg)
	}
	return rst
}
func get_pluginname(metric string) string {
	splitname := strings.Split(metric, "_")
	splitname = strings.Split(splitname[2], ".")
	return splitname[0]
}
