package main

import (
	"../.."
	"flag"
	"github.com/datastream/sessions"
	"log"
	"os"
	"os/signal"
	"syscall"
)

var (
	confFile = flag.String("c", "metrictools.json", "metrictools config file")
)

var sessionservice *sessions.RedisStore

func main() {
	flag.Parse()
	c, err := metrictools.ReadConfig(*confFile)
	if err != nil {
		log.Fatal("parse config file error: ", err)
	}

	n := &Notify{
		Setting:     c,
		msgChannel:  make(chan *metrictools.Message),
		exitChannel: make(chan int),
	}
	if err := n.Run(); err != nil {
		log.Fatal("fail to run metric notify task", err)
	}
	log.Println("start notify task")
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	n.Stop()
}
