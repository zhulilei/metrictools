package main

import (
	metrictools "../"
	"flag"
	"github.com/garyburd/redigo/redis"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

var (
	confFile = flag.String("conf", "metrictools.conf", "analyst config file")
)

func main() {
	flag.Parse()
	c, err := metrictools.ReadConfig(*confFile)
	if err != nil {
		log.Fatal("config parse error", err)
	}
	lookupdAddresses, _ := c["lookupd_addresses"]
	maxInFlight, _ := c["maxinflight"]
	emailAddress, _ := c["notify_email_address"]
	notifyChannel, _ := c["notify_channel"]
	notifyTopic, _ := c["notify_topic"]
	redisServer, _ := c["redis_server"]

	redisCon := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", redisServer)
		if err != nil {
			return nil, err
		}
		return c, err
	}
	max, _ := strconv.ParseInt(maxInFlight, 10, 32)
	lookupdlist := strings.Split(lookupdAddresses, ",")
	nt := &Notify{
		Pool:                redis.NewPool(redisCon, 3),
		notifyTopic:         notifyTopic,
		notifyChannel:       notifyChannel,
		maxInFlight:         int(max),
		nsqlookupdAddresses: lookupdlist,
		exitChannel:         make(chan int),
		msgChannel:          make(chan *metrictools.Message),
		EmailAddress:        emailAddress,
	}
	if err := nt.Run(); err != nil {
		log.Fatal(err)
	}
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	nt.Stop()
}
