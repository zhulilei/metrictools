package main

import (
	"github.com/datastream/metrictools/types"
)

type AlarmRequest struct {
	Alm types.Alarm
	Almact types.AlarmAction
	Act []types.Action
}
