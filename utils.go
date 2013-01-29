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
	Nm string
	Cl string
	Hs string
	V  float64
	Ts int64
}

type Metric struct {
	Record
	Retention string
	App       string
}

type StatisticRecord struct {
	Nm string  // trigger's name
	V  float64 // value
	Ts int64   // timestamp
}
type Trigger struct {
	Exp  string    // metric expressions
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

func NewMetric(s string) *Metric {
	splitstring := strings.Split(s, " ")
	this := NewLiteMetric(splitstring[0])

	if (len(splitstring) == 3) && (this != nil) {
		this.V, _ = strconv.ParseFloat(splitstring[1], 64)
		this.Ts, _ = strconv.ParseInt(splitstring[2], 10, 64)
		return this
	} else if len(splitstring) != 1 {
		log.Println("metric not recognized: ", s)
	}
	return nil
}

func NewLiteMetric(s string) *Metric {
	splitname := strings.Split(s, ".")
	if len(splitname) < 3 {
		if len(s) > 2 {
			log.Println("metrics not match", s)
		}
		return nil
	}
	var App string
	var Retention string
	var Name string
	var Hostname string
	var Colo string
	Hostname = splitname[len(splitname)-1]
	Colo = splitname[len(splitname)-2]
	var p int
	if rst, _ := regexp.MatchString(
		"(1sec|10sec|1min|5min|10min|15min)",
		splitname[0]); rst {
		Retention = splitname[0]
		App = splitname[1]
		p = 2
	} else {
		App = splitname[0]
		p = 1
	}

	for i := p; i < len(splitname)-2; i++ {
		if len(Name) > 0 {
			Name += "."
		}
		Name += splitname[i]
	}
	this := &Metric{
		App:       App,
		Retention: Retention,
		Record: Record{
			Nm: Name,
			Cl: Colo,
			Hs: Hostname,
		},
	}
	return this
}
