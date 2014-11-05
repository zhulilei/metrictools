package main

import (
	"../.."
	"flag"
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
	c, err := metrictools.ReadConfig(*confFile)
	if err != nil {
		log.Fatal("parse config file error: ", err)
	}

	p := &MetricDeliver{
		Setting:     c,
		exitChannel: make(chan int),
		msgChannel:  make(chan *metrictools.Message),
	}
	if err := p.Run(); err != nil {
		log.Fatal("fail to run metric process task", err)
	}
	log.Println("start process task")
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	p.Stop()
}
