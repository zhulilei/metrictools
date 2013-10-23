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
	redisServer, _ := c["data_redis_server"]
	redisAuth, _ := c["data_redis_auth"]
	configRedisServer, _ := c["config_redis_server"]
	configRedisAuth, _ := c["config_redis_auth"]
	fullDuration, _ := c["full_duration"]
	consensus, _ := c["consensus"]

	redisCon := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", redisServer)
		if err != nil {
			return nil, err
		}
		if _, err := c.Do("AUTH", redisAuth); err != nil {
			c.Close()
			return nil, err
		}
		return c, err
	}
	redisPool := redis.NewPool(redisCon, 3)
	defer redisPool.Close()
	// redis
	configRedisCon := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", configRedisServer)
		if err != nil {
			return nil, err
		}
		if _, err := c.Do("AUTH", configRedisAuth); err != nil {
			c.Close()
			return nil, err
		}
		return c, err
	}
	configRedisPool := redis.NewPool(configRedisCon, 3)
	defer configRedisPool.Close()
	r, err := nsq.NewReader(triggerTopic, triggerChannel)
	if err != nil {
		log.Fatal(err)
	}
	max, _ := strconv.ParseInt(maxInFlight, 10, 32)
	r.SetMaxInFlight(int(max))
	w := nsq.NewWriter(nsqdAddr)
	tt := &TriggerTask{
		writer:        w,
		dataService:   redisPool,
		configService: configRedisPool,
		topic:         notifyTopic,
		nsqdAddress:   nsqdAddr,
	}
	tt.FullDuration, _ = strconv.ParseInt(fullDuration, 10, 64)
	tt.Consensus, _ = strconv.Atoi(consensus)
	for i := 0; i < int(max); i++ {
		r.AddHandler(tt)
	}
	lookupdList := strings.Split(lookupdAddresses, ",")
	for _, addr := range lookupdList {
		log.Printf("lookupd addr %s", addr)
		err := r.ConnectToLookupd(addr)
		if err != nil {
			log.Fatal(err)
		}
	}
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	r.Stop()
	w.Stop()
}
