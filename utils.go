package metrictools

import (
	"errors"
	"log"
	"regexp"
	"strconv"
	"strings"
)

// MetricData will be stored in redis.
type MetricData struct {
	Value          float64 `json:"value", redis:"value"`
	DataSetType    string  `json:"dstype", redis:"dstype"`
	DataSetName    string  `json:"dsname", redis:"dsname"`
	Timestamp      int64   `json:"timestamp", redis:"timestamp"`
	Interval       float64 `json:"interval", redis:"interval"`
	Host           string  `json:"host", redis:"host"`
	Plugin         string  `json:"plugin", redis:"plugin"`
	PluginInstance string  `json:"plugin_instance", redis:"plugin_instance"`
	Type           string  `json:"type" redis:"type"`
	TypeInstance   string  `json:"type_instance", redis:"type_instance"`
	TTL            int     `json:"ttl", redis:"ttl"`
}

// Trigger define a statistic expression
type Trigger struct {
	IsExpression bool   `json:"is_expression", redis:"is_e"`
	Name         string `json:"name", redis:"name"`
	Role         string `json:"role", redis:"role"`
}

// NotifyAction define how to send notify
type NotifyAction struct {
	Uri        string `json:"uri", redis:"uri"`
	UpdateTime int64  `json:"update_time", redis:"update_time"`
	Repeat     int    `json:"repeat", redis:"repeat"`
	Count      int    `json:"count", redis:"count"`
}

// GetMetricName return metricdata name
func (m *MetricData) GetMetricName() string {
	metricName := m.Plugin
	if len(m.PluginInstance) > 0 {
		if matched, _ := regexp.MatchString(`^\d+$`, m.PluginInstance); matched {
			metricName += m.PluginInstance
		} else {
			metricName += "_" + m.PluginInstance
		}
	}
	if len(m.Type) > 0 && m.Type != m.Plugin {
		metricName += "." + m.Type
	}
	if len(m.TypeInstance) > 0 {
		if matched, _ := regexp.MatchString(`^\d+$`, m.TypeInstance); matched {
			metricName += m.TypeInstance
		} else {
			metricName += "_" + m.TypeInstance
		}
	}
	if m.DataSetName != "value" {
		metricName += "." + m.DataSetName
	}
	return metricName
}

// GetTimestampAndValue return timestamp and value
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

// GenerateTimeseries return metricdata's timestamp and value
func GenerateTimeseries(metricData []string) [][]interface{} {
	var timeserires [][]interface{}
	for _, val := range metricData {
		timestamp, value, err := GetTimestampAndValue(val)
		if err != nil {
			log.Println("invalid data", val)
			continue
		}
		timeserires = append(timeserires, []interface{}{timestamp, value})
	}
	return timeserires
}

type Message struct {
	Body       interface{}
	ErrorChannel chan error
}
