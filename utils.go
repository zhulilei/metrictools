package main

import (
	"bytes"
	"code.google.com/p/goprotobuf/proto"
	"encoding/binary"
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
