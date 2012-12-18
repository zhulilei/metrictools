package main

import (
	"github.com/datastream/metrictools"
)

type AlarmRequest struct {
	Alarm_info    metrictools.Alarm
	Alarm_actions []metrictools.AlarmAction
}
