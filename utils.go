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
	Expression string  `json:"expression"`
	Persist    bool    `json:"persist"`
	Relation   int     `json:"relation"`
	Interval   int     `json:"interval"`
	Period     int     `json:"period"`
	WValue     float64 `json:"warning"`
	EValue     float64 `json:"error"`
	Name       string  `json:"name"`
	Role       string  `json:"role"`
	Stat       int     `json:"stat"`
}

type Notify struct {
	Name  string  `json:"trigger_name"`
	Level int     `json:"level"`
	Value float64 `json:"value"`
}

type NotifyAction struct {
	Name       string `json:"name"`
	Repeat     int    `json:"repeat"`
	Uri        string `json:"uri"`
	UpdateTime int64  `json:"update_time"`
	Count      int    `json:"count"`
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
