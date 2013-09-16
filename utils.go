package metrictools

import (
	"errors"
	"log"
	"strconv"
	"strings"
)

const (
	LESS    = 1
	GREATER = 2
)

type MetricData struct {
	Value          float64 `json:"value", redis:"value"`
	DataSetType    string  `json:"dstype", redis:"dstype"`
	DataSetName    string  `json:"dsname", redis:"dsname"`
	Timestamp      int64   `json:"timestamp", redis:"-"`
	Interval       float64 `json:"interval", redis:"interval"`
	Host           string  `json:"host", redis:"host"`
	Plugin         string  `json:"plugin", redis:"plugin"`
	PluginInstance string  `json:"plugin_instance", redis:"plugin_instance"`
	Type           string  `json:"type" redis:"type"`
	TypeInstance   string  `json:"type_instance", redis:"type_instance"`
	TTL            int     `json:"ttl", redis:"ttl"`
}

type Trigger struct {
	Expression string  `json:"expression", redis:"exp"`
	Persist    bool    `json:"persist", redis:"persist"`
	Relation   int     `json:"relation", redis:"relation"`
	Interval   int     `json:"interval", redis:"interval"`
	Period     int     `json:"period", redis:"period"`
	WValue     float64 `json:"warning", redis:"warning"`
	EValue     float64 `json:"error", redis:"error"`
	Name       string  `json:"name", redis:"-"`
	Role       string  `json:"role", redis:"role"`
	Stat       int     `json:"stat", redis:"stat"`
}

type Notify struct {
	Name  string  `json:"trigger_name"`
	Level int     `json:"level"`
	Value float64 `json:"value"`
}

type NotifyAction struct {
	Name       string `json:"name", redis:"-"`
	Repeat     int    `json:"repeat", redis:"repeat"`
	Uri        string `json:"uri", redis:"uri"`
	UpdateTime int64  `json:"update_time", redis:"update_time"`
	Count      int    `json:"count", redis:"count"`
}

func (this *MetricData) GetMetricName() string {
	metric_name := strconv.Itoa(int(this.Interval)) + "_" + this.Plugin
	if len(this.PluginInstance) > 0 {
		metric_name += "_" + this.PluginInstance
	}
	if len(this.Type) > 0 {
		metric_name += "." + this.Type
	}
	if len(this.TypeInstance) > 0 {
		metric_name += "_" + this.TypeInstance
	}
	metric_name += "." + this.DataSetName
	return metric_name
}

func GetTimestampAndValue(key string) (int64, float64, error) {
	body := string(key)
	kv := strings.Split(body, ":")
	var t int64
	var v float64
	var err error
	if len(kv) == 2 {
		t, err = strconv.ParseInt(kv[0], 10, 64)
		v, err = strconv.ParseFloat(kv[1], 64)
	} else {
		err = errors.New("wrong data")
	}
	return t, v, err
}

func GenerateTimeseries(metric_data []string) [][]interface{} {
	var timeserires [][]interface{}
	for _, val := range metric_data {
		timestamp, value, err := GetTimestampAndValue(val)
		if err != nil {
			log.Println("invalid data", val)
			continue
		}
		timeserires = append(timeserires, []interface{}{timestamp, value})
	}
	return timeserires
}
