package metrictools

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
)

type Config struct {
	Global    map[string]string `json:"global"`
	Metric    map[string]string `json:"metric"`
	Trigger   map[string]string `json:"trigger"`
	Statistic map[string]string `json:"statistic"`
	Notify    map[string]string `json:"notify"`
	Redis     map[string]string `json:"redis"`
	web       map[string]string `json:"web"`
}

func ReadConfig(file string) (*Config, error) {
	var setting Config
	config_file, err := os.Open(file)
	config, err := ioutil.ReadAll(config_file)
	if err != nil {
		return nil, err
	}
	config_file.Close()
	if err := json.Unmarshal(config, &setting); err != nil {
		return nil, err
	}
	return &setting, nil
}
