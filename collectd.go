package metrictools

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
	for i := range this.Values {
		metric := &MetricData{
			Value:          this.Values[i],
			DataSetType:    this.DataSetTypes[i],
			DataSetName:    this.DataSetNames[i],
			Timestamp:      int64(this.Timestamp),
			Interval:       this.Interval,
			Host:           this.Host,
			Plugin:         this.Plugin,
			PluginInstance: this.PluginInstance,
			Type:           this.Type,
			TypeInstance:   this.TypeInstance,
			TTL:            int(this.Interval) * 3600,
		}
		metrics = append(metrics, metric)
	}
	return metrics
}
