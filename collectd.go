package metrictools

import "strconv"

type CollectdJSON struct {
	Values         []float64 `json:"values"`
	DSTypes        []string  `json:"dstypes"`
	DSNames        []string  `json:"dsnames"`
	TimeStamp      float64   `json:"time"`
	Interval       float64   `json:"interval"`
	Host           string    `json:"host"`
	Plugin         string    `json:"plugin"`
	PluginInstance string    `json:"plugin_instance"`
	Type           string    `json:"type"`
	TypeInstance   string    `json:"type_instance"`
}

func (this *CollectdJSON) GenNames() []string {
	base := strconv.Itoa(int(this.Interval)) + "_" + this.Plugin
	if len(this.PluginInstance) > 0 {
		base += "_" + this.PluginInstance
	}
	if len(this.Type) > 0 {
		base += "." + this.Type
	}
	if len(this.TypeInstance) > 0 {
		base += "_" + this.TypeInstance
	}
	var rst []string
	if len(this.DSNames) > 1 {
		for _, v := range this.DSNames {
			rst = append(rst, base+"."+v)
		}
	} else {
		rst = append(rst, base)
	}
	return rst
}
func (this *CollectdJSON) ToRecord() []*Record {
	keys := this.GenNames()
	var msgs []*Record
	for i := range this.Values {
		msg := &Record{
			Host:      this.Host,
			Key:       this.Host + "_" + keys[i],
			Value:     this.Values[i],
			Timestamp: int64(this.TimeStamp),
			TTL:       int(this.Interval) * 3 / 2,
			DSType:    this.DSTypes[i],
			Interval:  this.Interval,
		}
		msgs = append(msgs, msg)
	}
	return msgs
}
