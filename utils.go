package metrictools

import "labix.org/v2/mgo/bson"

const (
	AVG     = 1
	SUM     = 2
	MAX     = 3
	MIN     = 4
	EXP     = 5
	LESS    = 6
	GREATER = 7
)

type Record struct {
	Id        bson.ObjectId `bson:"_id,omitempty" json:"-"`
	Key       string        `bson:"k" json:"key"`
	Value     float64       `bson:"v" json:"value"`
	Timestamp int64         `bson:"t", json:"timestamp"`
	Host      string        `bson:"-" json:"host"`
	TTL       int           `bson:"-" json:"-"`
	DSType    string        `bson:"-" json:"-"`
	Interval  float64       `bson:"-" json:"-"`
}

type Trigger struct {
	Expression  string    `json:"expression"`
	TriggerType int       `json:"trigger_type"`
	Relation    int       `json:"relation"`
	Interval    int       `json:"interval"`
	Values      []float64 `json:"values"`
	Name        string    `json:"name"`
	Role        string    `json:"role"`
	Stat        int       `json:"stat"`
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
