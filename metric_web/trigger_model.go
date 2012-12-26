package main

import (
	"github.com/datastream/metrictools"
)

type TriggerRequest struct {
	trigger metrictools.Trigger
	actions []metrictools.NotifyAction
}
