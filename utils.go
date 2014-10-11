package metrictools

import (
	"code.google.com/p/goprotobuf/proto"
	"github.com/datastream/skyline"
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
func GenerateTimeseries(metricData []skyline.TimePoint) [][]interface{} {
	var timeserires [][]interface{}
	for _, kv := range metricData {
		timeserires = append(timeserires, []interface{}{kv.GetTimestamp(), kv.GetValue()})
	}
	return timeserires
}

type Message struct {
	Body         interface{}
	ErrorChannel chan error
}

type Request struct {
	Cmd          string
	Args         []interface{}
	ReplyChannel chan interface{}
}

type StoreEngine interface {
	Do(rtype string, cmd string, args... interface{}) (interface{}, error)
	Start()
	Stop()
}
func KeyValueEncode(key int64, value float64) ([]byte, error) {
	kv := &KeyValue{
		Timestamp: proto.Int64(key),
		Value:     proto.Float64(value),
	}
	record, err := proto.Marshal(kv)
	return record, err
}

func KeyValueDecode(record []byte) (KeyValue, error) {
	var kv KeyValue
	err := proto.Unmarshal(record, &kv)
	return kv, err
}

// ParseTimeSeries convert redis value to skyline's data format
func ParseTimeSeries(values []string) []skyline.TimePoint {
	var rst []skyline.TimePoint
	for _, val := range values {
		size := len(val)
		for i := 0; i < size; i += 18 {
			if (i + 18) > size {
				break
			}
			kv, err := KeyValueDecode([]byte(val[i : i+18]))
			if err != nil {
				continue
			}
			rst = append(rst, &kv)
		}
	}
	return rst
}
