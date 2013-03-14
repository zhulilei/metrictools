package metrictools

import (
	"log"
	"regexp"
	"strconv"
	"strings"
)

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
	H string  //host
	K string  //key
	V float64 //value
	T int64   //time
}

type RedisQuery struct {
	Key   string
	Value chan float64
}

type StatisticRecord struct {
	K string  // trigger's name
	V  float64 // value
	Ts int64   // timestamp
}

type Trigger struct {
	Exp  string    // expressions, h+":"+k / h2+":"+k
	T    int       // AVG, SUM, MAX, MIN
	P    int       // 1min, 5min, 15min
	J    int       // LESS, GREATER
	I    int       // check interval time: 1min, 5min, 15min
	V    []float64 // value
	R    bool      // insert into mongodb?
	Nm   string    // auto generate
	Pd   string    // blog, photo, reader, etc.
	Last int64     // last modify time
	Stat int       // last trigger stat
}

type Notify struct {
	Exp   string // metric expressions
	Level int    // 0 ok, 1 error, 2 critical
	Value float64
}

type NotifyAction struct {
	Exp   string // metric expressions
	Ir    bool   // repeated ? default: false ,send 3 times in 5mins
	Uri   string // email address, phone number, im id, mq info
	Last  int64  // last notify time
	Count int    // count in period time
}
