package main

import (
	metrictools "../"
	"flag"
	"github.com/bitly/go-nsq"
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
	redisServer, _ := c["data_redis_server"]
	configRedisServer, _ := c["config_redis_server"]

	redisCon := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", redisServer)
		if err != nil {
			return nil, err
		}
		return c, err
	}
	redisPool := redis.NewPool(redisCon, 3)
	defer redisPool.Close()

	configRedisCon := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", configRedisServer)
		if err != nil {
			return nil, err
		}
		return c, err
	}
	configRedisPool := redis.NewPool(configRedisCon, 3)
	defer configRedisPool.Close()

	w := nsq.NewWriter(nsqdAddr)
	metricDeliver := &MetricDeliver{
		dataService:   redisPool,
		configService: configRedisPool,
		writer:        w,
		triggerTopic:  triggerTopic,
		archiveTopic:  archiveTopic,
		nsqdAddr:      nsqdAddr,
	}
	max, _ := strconv.ParseInt(maxinflight, 10, 32)
	r, err := nsq.NewReader(metricTopic, metricChannel)
	if err != nil {
		log.Fatal(err)
	}
	r.SetMaxInFlight(int(max))
	for i := 0; i < int(max); i++ {
		r.AddHandler(metricDeliver)
	}
	lookupdlist := strings.Split(lookupdAddresses, ",")
	for _, addr := range lookupdlist {
		log.Printf("lookupd addr %s", addr)
		err := r.ConnectToLookupd(addr)
		if err != nil {
			log.Fatal(err)
		}
	}
	go metricDeliver.ScanTrigger()
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	r.Stop()
	w.Stop()
}
