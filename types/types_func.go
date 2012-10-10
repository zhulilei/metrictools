package types

import (
	"log"
	"regexp"
	"strconv"
	"strings"
)

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
		log.Println("metrics not match", s)
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
	if rst, _ := regexp.MatchString("(1sec|10sec|1min|5min|10min|15min)", splitname[0]); rst {
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
		App: App,
		Record: Record{
			Rt: Retention,
			Nm: Name,
			Cl: Colo,
			Hs: Hostname,
		},
	}
	return this
}

// return m/n
func Div_value(m, n []Record) []Record {
	var values map[int64]float64
	for i := range m {
		values[m[i].Ts/60] = m[i].V
	}
	for i := range n {
		values[n[i].Ts/60] /= n[i].V
	}
	var rst []Record
	for k, v := range values {
		var t Record
		t.V = v
		t.Ts = k * 60
		rst = append(rst, t)
	}
	return rst
}

func Avg_value(r []Record) float64 {
	return Sum_value(r) / float64(len(r))
}

func Sum_value(r []Record) float64 {
	var rst float64
	rst = 0
	for i := range r {
		rst += r[i].V
	}
	return rst
}

func Max_value(r []Record) float64 {
	var rst float64
	rst = 0
	for i := range r {
		if rst < r[i].V {
			rst = r[i].V
		}
	}
	return rst
}

func Min_value(r []Record) float64 {
	var rst float64
	rst = r[0].V
	for i := range r {
		if rst > r[i].V {
			rst = r[i].V
		}
	}
	return rst
}

func Judge_value(S Alarm, value float64) int {
	switch S.J {
	case LESS:
		{
			if value < S.V {
				return 1
			}
		}
	case GREATER:
		{
			if value > S.V {
				return 1
			}
		}
	}
	return 0
}
