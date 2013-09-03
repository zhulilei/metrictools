package metrictools

import (
	"errors"
	"strconv"
	"strings"
)

const (
	LESS    = 1
	GREATER = 2
)

type Record struct {
	Key       string
	Value     float64
	Timestamp int64
	Host      string
	TTL       int
	DSType    string
	Interval  float64
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
