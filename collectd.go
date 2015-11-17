package metrictools

import (
	"fmt"
)

// CollectdJSON is collectd's json data format
type CollectdJSON struct {
	Values         []float64 `json:"values"`
	DataSetTypes   []string  `json:"dstypes"`
	DataSetNames   []string  `json:"dsnames"`
	Timestamp      float64   `json:"time"`
	Interval       float64   `json:"interval"`
	Host           string    `json:"host"`
	Plugin         string    `json:"plugin"`
	PluginInstance string    `json:"plugin_instance"`
	Type           string    `json:"type"`
	TypeInstance   string    `json:"type_instance"`
}

func (c *CollectdJSON) GetMetricName(index int, user string) string {
	metricName := fmt.Sprintf("{\"plugin\":\"%s\",\"host\":\"%s\",\"user\":\"%s\"", c.Plugin, c.Host, user)
	if len(c.PluginInstance) > 0 {
		metricName = fmt.Sprintf("%s,\"plugin_instance\":\"%s\"", metricName, c.PluginInstance)
	}
	metricName = fmt.Sprintf("%s,\"interval\":\"%s\"", metricName, c.Interval)
	if len(c.Type) > 0 {
		metricName = fmt.Sprintf("%s,\"type\":\"%s\"", metricName, c.Type)
	}
	if len(c.TypeInstance) > 0 {
		metricName = fmt.Sprintf("%s,\"type_instance\":\"%s\"", metricName, c.TypeInstance)
	}
	metricName = fmt.Sprintf("%s,\"field\":\"%s\"}", metricName, c.DataSetNames[index])
	return metricName
}

func (c *CollectdJSON) GetMetricRate(value float64, timestamp int64, index int) float64 {
	var nValue float64
	if c.DataSetTypes[index] == "counter" || c.DataSetTypes[index] == "derive" {
		nValue = (c.Values[index] - value) / float64(int64(c.Timestamp)-timestamp)
		if nValue < 0 {
			nValue = 0
		}
	} else {
		nValue = c.Values[index]
	}
	return nValue
}
