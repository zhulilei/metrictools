package main

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
	"log"
	"regexp"
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

// GenerateTimeseries return metricdata's timestamp and value
func GenerateTimeseries(metricData []string) [][]interface{} {
	var timeserires [][]interface{}
	for _, val := range metricData {
		timestamp, value, err := KeyValueDecode([]byte(val))
		if err != nil {
			log.Println("invalid data", val)
			continue
		}
		timeserires = append(timeserires, []interface{}{timestamp, value})
	}
	return timeserires
}

type Message struct {
	Body         interface{}
	ErrorChannel chan error
}

func KeyValueEncode(key int64, value float64) ([]byte, error) {
	var err error
	var record []byte
	buf := new(bytes.Buffer)
	err = binary.Write(buf, binary.LittleEndian, value)
	if err == nil {
		kv := &KeyValue{
			Timestamp: proto.Int64(key),
			Value:     buf.Bytes(),
		}
		record, err = proto.Marshal(kv)
	}
	return record, err
}

func KeyValueDecode(record []byte) (int64, float64, error) {
	var kv KeyValue
	var err error
	var value float64
	err = proto.Unmarshal(record, &kv)
	if err == nil {
		buf := bytes.NewReader(kv.GetValue())
		err = binary.Read(buf, binary.LittleEndian, &value)
	}
	return kv.GetTimestamp(), value, err
}
