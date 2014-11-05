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

	s := &StatisticTask{
		Setting:          c,
		exitChannel:      make(chan int),
		statisticChannel: make(chan string),
	}
	if err := s.Run(); err != nil {
		log.Fatal("fail to run metric statistic task", err)
	}
	log.Println("start statistic task")
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	s.Stop()
}
