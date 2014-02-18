package main

import (
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

var queryservice *WebQueryPool

var sessionservice *sessions.RedisStore

type MetricTask interface {
	Stop()
}

func main() {
	flag.Parse()
	c, err := ReadConfig(*confFile)
	if err != nil {
		log.Fatal("parse config file error: ", err)
	}

	if len(c.Modes) == 0 {
		c.Modes = []string{"archive", "notify", "process", "statistic", "webapi"}
	}
	var tasks []MetricTask
	for _, v := range c.Modes {
		switch v {
		case "archive":
			a := &DataArchive{
				Setting:     c,
				msgChannel:  make(chan *Message),
				exitChannel: make(chan int),
			}
			if err := a.Run(); err != nil {
				log.Fatal("fail to run metric archive task", err)
			}
			tasks = append(tasks, a)
			log.Println("start archive task")
		case "notify":
			n := &Notify{
				Setting:     c,
				msgChannel:  make(chan *Message),
				exitChannel: make(chan int),
			}
			if err := n.Run(); err != nil {
				log.Fatal("fail to run metric notify task", err)
			}
			tasks = append(tasks, n)
			log.Println("start notify task")
		case "process":
			p := &MetricDeliver{
				Setting:     c,
				exitChannel: make(chan int),
				msgChannel:  make(chan *Message),
			}
			if err := p.Run(); err != nil {
				log.Fatal("fail to run metric process task", err)
			}
			tasks = append(tasks, p)
			log.Println("start process task")
		case "statistic":
			s := &TriggerTask{
				Setting:     c,
				exitChannel: make(chan int),
				msgChannel:  make(chan *Message),
			}
			if err := s.Run(); err != nil {
				log.Fatal("fail to run metric statistic task", err)
			}
			tasks = append(tasks, s)
			log.Println("start statistic task")
		case "webapi":
			queryservice = &WebQueryPool{
				Setting:      c,
				exitChannel:  make(chan int),
				queryChannel: make(chan *RedisQuery),
			}
			go queryservice.Run()
			tasks = append(tasks, queryservice)
			log.Println("start webapi queryservice")
			sessionservice = sessions.NewRedisStore("tcp", queryservice.RedisServer, "")
			go sessionservice.Run()
			tasks = append(tasks, sessionservice)
			log.Println("start webapi sessionservice")
		default:
			log.Println(v, " is not supported mode")
		}
	}

	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	for _, t := range tasks {
		t.Stop()
	}
}
