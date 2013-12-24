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
	nsqdAddr, _ := c["nsqd_addr"]
	maxInFlight, _ := c["maxinflight"]
	triggerChannel, _ := c["trigger_channel"]
	triggerTopic, _ := c["trigger_topic"]
	notifyTopic, _ := c["notify_topic"]
	redisServer, _ := c["redis_server"]
	fullDuration, _ := c["full_duration"]
	consensus, _ := c["consensus"]

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
	lookupdList := strings.Split(lookupdAddresses, ",")
	tt := &TriggerTask{
		Pool:                redisPool,
		nsqdAddr:            nsqdAddr,
		triggerTopic:        triggerTopic,
		triggerChannel:      triggerChannel,
		notifyTopic:         notifyTopic,
		nsqlookupdAddresses: lookupdList,
		maxInFlight:         int(max),
		exitChannel:         make(chan int),
		msgChannel:          make(chan *metrictools.Message),
		nsqdAddress:         nsqdAddr,
	}
	tt.FullDuration, _ = strconv.ParseInt(fullDuration, 10, 64)
	tt.Consensus, _ = strconv.Atoi(consensus)
	if err := tt.Run(); err != nil {
		log.Fatal(err)
	}
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	tt.Stop()
}
