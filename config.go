package metrictools

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

type Config map[string]string

func ReadConfig(file string) (Config, error) {
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
	return setting, nil
}
