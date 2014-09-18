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

	s := &SkylineTask{
		Setting:     c,
		exitChannel: make(chan int),
		msgChannel:  make(chan *metrictools.Message),
	}
	if err := s.Run(); err != nil {
		log.Fatal("fail to run metric statistic task", err)
	}
	log.Println("start skyline task")
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	s.Stop()
}
