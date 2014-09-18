package metrictools

import (
	"github.com/fzzy/radix/redis"
	"regexp"
	"strconv"
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

func (c *CollectdJSON) GetMetricName(index int) string {
	metricName := c.Host + "_" + c.Plugin
	if len(c.PluginInstance) > 0 {
		if matched, _ := regexp.MatchString(`^\d+$`, c.PluginInstance); matched {
			metricName += c.PluginInstance
		} else {
			metricName += "_" + c.PluginInstance
		}
	}
	tSize := len(c.Type)
	pSize := len(c.Plugin)
	if tSize > 0 {
		if pSize <= tSize && c.Type[:pSize] == c.Plugin {
			metricName += c.Type[pSize:]
		} else {
			metricName += "." + c.Type
		}
	}
	if len(c.TypeInstance) > 0 {
		if matched, _ := regexp.MatchString(`^\d+$`, c.TypeInstance); matched {
			metricName += c.TypeInstance
		} else {
			metricName += "_" + c.TypeInstance
		}
	}
	if c.DataSetNames[index] != "value" {
		metricName += "." + c.DataSetNames[index]
	}
	return metricName
}

func GetMetricRate(key string, value float64, timestamp int64, dataType string, client *redis.Client) (float64, error) {
	var nValue float64
	rst, err := client.Cmd("HMGET", key, "value", "timestamp").List()
	if err != nil {
		return 0, err
	}
	var t int64
	var v float64
	t, _ = strconv.ParseInt(rst[0], 0, 64)
	v, err = strconv.ParseFloat(rst[1], 64)
	if err != nil {
		return 0, err
	}
	if dataType == "counter" || dataType == "derive" {
		nValue = (value - v) / float64(timestamp-t)
		if nValue < 0 {
			nValue = 0
		}
	} else {
		nValue = value
	}
	return nValue, err
}
