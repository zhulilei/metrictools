package main

import (
	"encoding/json"
	"flag"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	confFile = flag.String("c", "metrictools.json", "metrictools config file")
)

func main() {
	flag.Parse()
	w, err := ReadConfig(*confFile)
	if err != nil {
		log.Fatal("parse config file error: ", err)
	}
	go w.Run()
	log.Println("start webapi sessionservice")
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	w.Stop()
}

// ReadConfig used to read json to config
func ReadConfig(file string) (*WebService, error) {
	configFile, err := os.Open(file)
	config, err := ioutil.ReadAll(configFile)
	if err != nil {
		return nil, err
	}
	configFile.Close()
	setting := &WebService{}
	if err := json.Unmarshal(config, &setting); err != nil {
		return nil, err
	}
	return setting, err
}
