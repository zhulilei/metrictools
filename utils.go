package main

import (
	"code.google.com/p/goprotobuf/proto"
	"log"
)

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
	kv := &KeyValue{
		Timestamp: proto.Int64(key),
		Value:     proto.Float64(value),
	}
	record, err := proto.Marshal(kv)
	return record, err
}

func KeyValueDecode(record []byte) (int64, float64, error) {
	var kv KeyValue
	err := proto.Unmarshal(record, &kv)
	return kv.GetTimestamp(), kv.GetValue(), err
}
