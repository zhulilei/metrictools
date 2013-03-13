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

func read_condig(file string) *Config {
	var setting Config
	config_file, err := os.Open(file)
	config, err := ioutil.ReadAll(config_file)
	if err != nil {
		log.Println(err)
		return nil
	}
	config_file.Close()
	if err := json.Unmarshal(config, &setting); err != nil {
		log.Println(err)
		return nil
	}
	return &setting
}
