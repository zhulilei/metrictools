package main

import (
	"strings"
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

// GenerateMetricData convert json to metricdata
func (c *CollectdJSON) GenerateMetricData() []*MetricData {
	var metrics []*MetricData
	var host string
	for i := range c.Values {
		host = strings.Replace(c.Host, "-", ".", -1)
		metric := &MetricData{
			Value:          c.Values[i],
			DataSetType:    c.DataSetTypes[i],
			DataSetName:    c.DataSetNames[i],
			Timestamp:      int64(c.Timestamp),
			Interval:       c.Interval,
			Host:           host,
			Plugin:         c.Plugin,
			PluginInstance: c.PluginInstance,
			Type:           c.Type,
			TypeInstance:   c.TypeInstance,
			TTL:            int(c.Interval) * 180,
		}
		metrics = append(metrics, metric)
	}
	return metrics
}
