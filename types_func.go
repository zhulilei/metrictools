package metrictools

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
