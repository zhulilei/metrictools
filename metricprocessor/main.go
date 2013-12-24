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
	nsqdAddr, _ := c["nsqd_addr"]
	maxinflight, _ := c["maxinflight"]
	metricChannel, _ := c["metric_channel"]
	metricTopic, _ := c["metric_topic"]
	triggerTopic, _ := c["trigger_topic"]
	archiveTopic, _ := c["archive_topic"]
	redisServer, _ := c["redis_server"]

	redisCon := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", redisServer)
		if err != nil {
			return nil, err
		}
		return c, err
	}
	redisPool := redis.NewPool(redisCon, 3)

	lookupdlist := strings.Split(lookupdAddresses, ",")
	max, _ := strconv.ParseInt(maxinflight, 10, 32)
	metricDeliver := &MetricDeliver{
		Pool:                redisPool,
		triggerTopic:        triggerTopic,
		archiveTopic:        archiveTopic,
		nsqdAddr:            nsqdAddr,
		nsqlookupdAddresses: lookupdlist,
		maxInFlight:         int(max),
		metricTopic:         metricTopic,
		metricChannel:       metricChannel,
		exitChannel:         make(chan int),
		msgChannel:          make(chan *metrictools.Message),
	}
	if err := metricDeliver.Run(); err != nil {
		log.Fatal(err)
	}
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	metricDeliver.Stop()
}
