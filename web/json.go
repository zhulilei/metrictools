package main

import (
	"encoding/json"
	"github.com/datastream/metrictools/types"
	"log"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

func json_metrics_value(m []types.Record, app string) *string {
	var rst string
	metrics := make(map[string]string)
	for i := range m {
		name := m[i].Rt + "." + app + "." + m[i].Nm + "." + m[i].Cl + "." + m[i].Hs
		if _, ok := metrics[name]; ok {
			metrics[name] += ","
			metrics[name] += "[" + strconv.FormatInt(m[i].Ts, 10) + "," + strconv.FormatFloat(m[i].V, 'f', -1, 64) + "]"
		} else {
			metrics[name] = "[" + strconv.FormatInt(m[i].Ts, 10) + "," + strconv.FormatFloat(m[i].V, 'f', -1, 64) + "]"
		}
	}
	var keys []string
	for k, _ := range metrics {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for l := range keys {
		if len(rst) > 0 {
			rst += ","
		}
		rst += "{\"key\": \"" + keys[l] + "\",\"values\": ["
		rst += metrics[keys[l]]
		rst += "]},"
	}
	return &rst
}

func gen_value(values map[int64]float64, name string) string {
	var rst string
	rst += "{\"key\": \"" + name + "\",\"values\": ["
	for k, v := range values {
		rst += "[" + strconv.FormatInt(k*60, 10) + "," + strconv.FormatFloat(v, 'f', -1, 64) + "],"
	}

	rst = rst[:len(rst)-1] + "]},"
	return rst
}

type out_host_json struct {
	Key    string
	Url    string
	Values []*mid_host_json
}
type mid_host_json struct {
	Key     string
	MValues []*in_host_json
}

type in_host_json struct {
	Key string
	Url string
}

func json_host_list(h []types.Host) *string {
	var m []types.Metric
	for i := range h {
		m = append(m, *types.NewLiteMetric(h[i].Metric))
	}
	hosts := make(map[string][]string)
	times := make(map[string][]string)
	colos := make(map[string][]string)
	for i := range m {
		name := m[i].Rt + "." + m[i].App + "." + m[i].Nm + "." + m[i].Cl + "." + m[i].Hs
		hosts[m[i].Hs] = append(hosts[m[i].Hs], name)
		times[m[i].Rt] = append(times[m[i].Rt], name)
		colos[m[i].Cl] = append(colos[m[i].Cl], name)

	}
	out := &out_host_json{
		Key: "Metrics List",
	}
	for k, v := range hosts {
		sort.Strings(v)
		mid := &mid_host_json{
			Key: k,
		}
		for i := range v {
			if i != 0 {
				if v[i] == v[i-1] {
					continue
				}
			}
			inter := &in_host_json{
				Key: get_metricname_without_hostname(v[i]),
				Url: "/monitor?metricsname=" + v[i] + "&host=" + get_hostname(v[i]),
			}
			mid.MValues = append(mid.MValues, inter)
		}
		out.Values = append(out.Values, mid)
	}
	for k, v := range times {
		sort.Strings(v)
		mid := &mid_host_json{
			Key: k,
		}
		for i := range v {
			if i != 0 {
				if v[i] == v[i-1] {
					continue
				}
			}
			inter := &in_host_json{
				Key: get_metricname_without_retention(v[i]),
				Url: "/monitor?metricsname=" + v[i] + "&host=" + get_hostname(v[i]),
			}
			mid.MValues = append(mid.MValues, inter)
		}
		out.Values = append(out.Values, mid)
	}
	for k, v := range colos {
		sort.Strings(v)
		mid := &mid_host_json{
			Key: k,
		}
		for i := range v {
			if i != 0 {
				if v[i] == v[i-1] {
					continue
				}
			}
			inter := &in_host_json{
				Key: get_metricname_without_colo(v[i]),
				Url: "/monitor?metricsname=" + v[i] + "&host=" + get_hostname(v[i]),
			}
			mid.MValues = append(mid.MValues, inter)
		}
		out.Values = append(out.Values, mid)
	}
	var outlist []*out_host_json
	outlist = append(outlist, out)
	j_s, err := json.Marshal(outlist)
	if err != nil {
		log.Println("encode json error")
	}
	rst := strings.Replace(strings.ToLower(string(j_s)), "mvalues", "_values", -1)
	return &rst
}

func get_hostname(m string) string {
	rst := strings.Split(m, ".")
	return rst[len(rst)-1]
}

func get_metricname_without_hostname(m string) string {
	splitname := strings.Split(m, ".")
	var name string
	for i := 0; i < len(splitname)-1; i++ {
		if i == 1 {
			continue
		}
		if len(name) > 0 {
			name += "."
		}
		name += splitname[i]
	}
	return name
}

func get_metricname_without_retention(m string) string {
	splitname := strings.Split(m, ".")
	var name string
	for i := 2; i < len(splitname)-1; i++ {
		if len(name) > 0 {
			name += "."
		}
		name += splitname[i]
	}
	return name
}

func get_metricname_without_colo(m string) string {
	splitname := strings.Split(m, ".")
	var name string
	for i := 0; i < len(splitname)-1; i++ {
		if (i == (len(splitname) - 2)) || (i == 1) {
			continue
		}
		if len(name) > 0 {
			name += "."
		}
		name += splitname[i]
	}
	return name
}

func json_host_type(h []types.Host, host string) *string {
	host_type := make(map[string][]string)
	for i := range h {
		host_type[get_type(h[i])] = append(host_type[get_type(h[i])], h[i].Metric)
	}
	var keys []string
	for k, _ := range host_type {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	rst := "[{\"key\":\"" + host + "\",\"values\":["
	for l := range keys {
		sort.Strings(host_type[keys[l]])
		rst += "{\"key\":\"" + keys[l] + "\","
		var metrics string
		switch keys[l] {
		case "cpu":
			{
				rst += "\"_values\":["
				c := gen_cpu(host_type[keys[l]], host)
				rst += c[:len(c)-1]
				rst += "]"
			}
		case "partion":
			{
				rst += "\"_values\":["
				c := gen_partion(host_type[keys[l]], host)
				rst += c[:len(c)-1]
				rst += "]"
			}
		case "apache":
			{
				rst += "\"_values\":["
				c := gen_apache(host_type[keys[l]], host)
				rst += c[:len(c)-1]
				rst += "]"
			}
		case "disk":
			{
				rst += "\"_values\":["
				c := gen_disk(host_type[keys[l]], host)
				rst += c[:len(c)-1]
				rst += "]"
			}
		case "interface":
			{
				rst += "\"_values\":["
				c := gen_interface(host_type[keys[l]], host)
				rst += c[:len(c)-1]
				rst += "]"
			}
		default:
			{
				for i := range host_type[keys[l]] {
					metrics += host_type[keys[l]][i] + ","
				}
				rst += "\"url\":\"/monitor?metricsname=" + metrics[:len(metrics)-1] + "&host=" + host + "&type=" + keys[l] + "\""
			}
		}
		rst += "},"
	}
	rst = rst[:len(rst)-1] + "]}]"
	return &rst
}
func gen_cpu(v []string, host string) string {
	cpus := make(map[string][]string)
	for i := range v {
		reg, _ := regexp.Compile("[0-9]{1,2}.cpu")
		st := reg.FindString(v[i])
		st_list := strings.Split(st, ".")
		st = st_list[1] + st_list[0]
		cpus[st] = append(cpus[st], v[i])
	}
	return sort_json(cpus, host, "cpu")
}
func gen_partion(v []string, host string) string {
	partion := make(map[string][]string)
	for i := range v {
		reg, _ := regexp.Compile("partion.*(free|used|used_percent|total)")
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
func gen_apache(v []string, host string) string {
	apache := make(map[string][]string)
	for i := range v {
		reg, _ := regexp.Compile("apache_(scoreboard|connection|byte|request|idle)")
		st := reg.FindString(v[i])
		apache[st] = append(apache[st], v[i])
	}
	return sort_json(apache, host, "apache")
}
func gen_disk(v []string, host string) string {
	disk := make(map[string][]string)
	for i := range v {
		reg, _ := regexp.Compile("sd[a-z]{1,2}")
		st := reg.FindString(v[i])
		disk[st] = append(disk[st], v[i])
	}
	return sort_json(disk, host, "diskio")
}
func gen_interface(v []string, host string) string {
	eths := make(map[string][]string)
	for i := range v {
		reg, _ := regexp.Compile("((eth|bond|br)[0-9]{1,2}|lo).(tx|rx|if)")
		st := reg.FindString(v[i])
		st_list := strings.Split(st, ".")
		st = st_list[0]
		eths[st] = append(eths[st], v[i])
	}
	return sort_json(eths, host, "network")
}
func sort_json(arrary map[string][]string, host string, data_type string) string {
	var rst string
	var keys []string
	for k, _ := range arrary {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for l := range keys {
		rst += "{\"key\":\"" + keys[l] + "\","
		var metrics string
		sort.Strings(arrary[keys[l]])
		for i := range arrary[keys[l]] {
			metrics += arrary[keys[l]][i] + ","
		}
		rst += "\"url\":\"/monitor?metricsname=" + metrics[:len(metrics)-1] + "&host=" + host + "&type=" + data_type + "\"},"
	}
	return rst
}
func get_type(h types.Host) string {
	splitname := strings.Split(h.Metric, ".")
	return splitname[1]
}
