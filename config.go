package metrictools

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// Config is metrictools config struct
type Setting struct {
	InflexdbAddress    string   `json:"inflexdb_address"`
	InflexdbUser       string   `json:"inflexdb_user"`
	InflexdbPassword   string   `json:"inflexdb_password"`
	InflexdbDatabase   string   `json:"inflexdb_database"`
	NsqdAddress        string   `json:"nsqd_addr"`
	LookupdAddresses   []string `json:"lookupd_addresses"`
	MetricTopic        string   `json:"metric_topic"`
	MetricChannel      string   `json:"metric_channel"`
	SkylineTopic       string   `json:"skyline_topic"`
	SkylineChannel     string   `json:"skyline_channel"`
	TriggerTopic       string   `json:"trigger_topic"`
	TriggerChannel     string   `json:"trigger_channel"`
	ArchiveTopic       string   `json:"archive_topic"`
	ArchiveChannel     string   `json:"archive_channel"`
	NotifyTopic        string   `json:"notify_topic"`
	NotifyChannel      string   `json:"notify_channel"`
	NotifyEmailAddress string   `json:"notify_email_address"`
	RedisServer        string   `json:"redis_server"`
	FullDuration       int64    `json:"full_duration"`
	MinDuration        int64    `json:"min_duration"`
	Consensus          int      `json:"consensus"`
	MaxInFlight        int      `json:"maxinflight"`
	ListenAddress      string   `json:"listen_address"`
	SessionName        string   `json:"session_name"`
}

// ReadConfig used to read json to config
func ReadConfig(file string) (*Setting, error) {
	configFile, err := os.Open(file)
	config, err := ioutil.ReadAll(configFile)
	if err != nil {
		return nil, err
	}
	configFile.Close()
	setting := &Setting{}
	if err := json.Unmarshal(config, &setting); err != nil {
		return nil, err
	}
	return setting, err
}
