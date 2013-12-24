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
	confFile = flag.String("conf", "metrictools.json", "metrictools config file")
)

func main() {
	flag.Parse()
	c, err := metrictools.ReadConfig(*confFile)
	if err != nil {
		log.Fatal("config parse error", err)
	}
	lookupdAddresses, _ := c["lookupd_addresses"]
	maxInFlight, _ := c["archivemaxinflight"]
	redisServer, _ := c["redis_server"]
	archiveChannel, _ := c["archive_channel"]
	archiveTopic, _ := c["archive_topic"]

	redisCon := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", redisServer)
		if err != nil {
			return nil, err
		}
		return c, err
	}
	redisPool := redis.NewPool(redisCon, 3)
	defer redisPool.Close()

	max, _ := strconv.ParseInt(maxInFlight, 10, 32)
	lookupdlist := strings.Split(lookupdAddresses, ",")
	dr := &DataArchive{
		Pool:                redisPool,
		maxInFlight:         int(max),
		archiveTopic:        archiveTopic,
		archiveChannel:      archiveChannel,
		nsqlookupdAddresses: lookupdlist,
		msgChannel:          make(chan *Message),
		exitChannel:         make(chan int),
	}
	dr.Run()
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	dr.Stop()
}
