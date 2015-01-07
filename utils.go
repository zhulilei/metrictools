package metrictools

import (
	"github.com/datastream/skyline"
	"github.com/golang/protobuf/proto"
)

// MetricData
type Metric struct {
	Name          string
	LastTimestamp int64
	LastValue     float64
	ArchiveTime   int64
	RateValue     float64
	Mtype         string
	TTL           int64
}

// Trigger define a statistic expression
type Trigger struct {
	Name         string `json:"name"`
	Owner        string `json:"owner"`
	LastTime     int64  `json:"-"`
	IsExpression bool   `json:"is_expression"`
}

// NotifyAction define how to send notify
type NotifyAction struct {
	Name string `json:"-"`
	Uri  string `json:"uri"`
}

// User define user
type User struct {
	Name       string
	Password   string
	Permission string
	Group      string
	Role       string
}

// AccessToken define token
type AccessToken struct {
	Name       string
	UserName   string
	SecretKey  string
	Permission string
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
	SetAdd(set string, key string) error
	SetDelete(set string, key string) error
	GetSet(name string) ([]string, error)
	DeleteData(keys ...interface{}) error
	GetValues(keys ...interface{}) ([]string, error)
	AppendKeyValue(key string, value interface{}) error
	SetKeyValue(key string, value interface{}) error
	SetTTL(key string, ttl int64) error
	SetAttr(key string, attr string, value interface{}) error
	GetNotifyAction(name string) (NotifyAction, error)
	SaveNotifyAction(notifyAction NotifyAction) error
	GetTrigger(name string) (Trigger, error)
	SaveTrigger(trigger Trigger) error
	GetMetric(name string) (Metric, error)
	GetUser(name string) (User, error)
	GetToken(accessKey string) (AccessToken, error)
	RunTask()
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
