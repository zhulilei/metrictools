package main

import (
	"github.com/datastream/metrictools"
)

type AlarmRequest struct {
	Alarm_info    metrictools.Trigger
	Alarm_actions []metrictools.AlarmAction
}
