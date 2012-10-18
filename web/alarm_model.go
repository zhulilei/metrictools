package main

import (
	"github.com/datastream/metrictools"
)

type AlarmRequest struct {
	Alm    metrictools.Alarm
	Almact metrictools.AlarmAction
	Act    []metrictools.Action
}
