package metrictools

import (
	"strings"
)

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

func (this *CollectdJSON) GenerateMetricData() []*MetricData {
	var metrics []*MetricData
	var host string
	for i := range this.Values {
		host = strings.Replace(this.Host, "-", ".", -1)
		metric := &MetricData{
			Value:          this.Values[i],
			DataSetType:    this.DataSetTypes[i],
			DataSetName:    this.DataSetNames[i],
			Timestamp:      int64(this.Timestamp),
			Interval:       this.Interval,
			Host:           host,
			Plugin:         this.Plugin,
			PluginInstance: this.PluginInstance,
			Type:           this.Type,
			TypeInstance:   this.TypeInstance,
			TTL:            int(this.Interval) * 180,
		}
		metrics = append(metrics, metric)
	}
	return metrics
}
