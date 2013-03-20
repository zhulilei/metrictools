package main

import (
	metrictools "../"
)

type TriggerRequest struct {
	trigger metrictools.Trigger
	actions []metrictools.NotifyAction
}
