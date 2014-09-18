package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"../.."
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

	a := &DataArchive{
		Setting:     c,
		msgChannel:  make(chan *metrictools.Message),
		exitChannel: make(chan int),
	}
	if err := a.Run(); err != nil {
		log.Fatal("fail to run metric archive task", err)
	}

	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	a.Stop()
}
