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

type KeyValue struct {
	Timestamp int64   `json:"t"`
	Value     float64 `json:"v"`
}

type Trigger struct {
	Expression  string    `bson:"e" json:"expression"`
	TriggerType int       `bson:"t" json:"trigger_type"`
	Period      int       `bson:"p" json:"period"`
	Relation    int       `bson:"r" json:"relation"`
	Interval    int       `bson:"i" json:"interval"`
	Values      []float64 `bson:"v" json:"values"`
	Insertable  bool      `bson:"i" json:"insert_able"`
	Name        string    `bson:"n" json:"name"`
	Production  string    `bson:"pd" json:"production"`
	Stat        int       `bson:"st" json:"stat"`
	UpdateTime  int64     `bson:"u" json:"update_time"`
}

type Notify struct {
	Name  string  `bson:"n" json:"trigger_name"`
	Level int     `bson:"l" json:"level"`
	Value float64 `bson:"v" json:"value"`
}

type NotifyAction struct {
	Name       string `bson:"n" json:"trigger_name"`
	Repeat     int    `bson:"r" json:"repeat"`
	Uri        string `bson:"uri" json:"uri"`
	UpdateTime int64  `bson:"u" json:"update_time"`
	Count      int    `bson:"c" json:"count"`
}
