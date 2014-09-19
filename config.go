package metrictools

import (
	"encoding/json"
	"github.com/goinggo/mapstructure"
	"io/ioutil"
	"os"
)

// Config is metrictools config struct
type Setting struct {
	NsqdAddress        string   `jpath:"nsqd_addr"`
	LookupdAddresses   []string `jpath:"lookupd_addresses"`
	MetricTopic        string   `jpath:"metric_topic"`
	MetricChannel      string   `jpath:"metric_channel"`
	SkylineTopic       string   `jpath:"skyline_topic"`
	SkylineChannel     string   `jpath:"skyline_channel"`
	TriggerTopic       string   `jpath:"trigger_topic"`
	TriggerChannel     string   `jpath:"trigger_channel"`
	ArchiveTopic       string   `jpath:"archive_topic"`
	ArchiveChannel     string   `jpath:"archive_channel"`
	NotifyTopic        string   `jpath:"notify_topic"`
	NotifyChannel      string   `jpath:"notify_channel"`
	NotifyEmailAddress string   `jpath:"notify_email_address"`
	RedisServer        string   `jpath:"redis_server"`
	FullDuration       int64    `jpath:"full_duration"`
	MinDuration        int64    `jpath:"min_duration"`
	Consensus          int      `jpath:"consensus"`
	MaxInFlight        int      `jpath:"maxinflight"`
	ListenAddress      string   `jpath:"listen_address"`
	SessionName        string   `jpath:"session_name"`
	Network            string   `jpath:"network"`
}

// ReadConfig used to read json to config
func ReadConfig(file string) (*Setting, error) {
	configFile, err := os.Open(file)
	config, err := ioutil.ReadAll(configFile)
	if err != nil {
		return nil, err
	}
	configFile.Close()
	docMap := make(map[string]interface{})
	if err := json.Unmarshal(config, &docMap); err != nil {
		return nil, err
	}
	setting := &Setting{}
	err = mapstructure.DecodePath(docMap, setting)
	if setting.MinDuration == 0 {
		setting.MinDuration = 3600 * 3
	}
	if setting.FullDuration == 0 {
		setting.MinDuration = 3600 * 24
	}
	if setting.Network == "" {
		setting.Network = "tcp"
	}
	return setting, err
}
