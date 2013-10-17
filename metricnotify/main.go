package main

import (
	metrictools "../"
	"flag"
	nsq "github.com/bitly/go-nsq"
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
	configRedisServer, _ := c["config_redis_server"]
	configRedisAuth, _ := c["config_redis_auth"]

	redisCon := func() (redis.Conn, error) {
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
	redisPool := redis.NewPool(redisCon, 3)
	defer redisPool.Close()
	nt := &Notify{
		Pool:         redisPool,
		EmailAddress: emailAddress,
	}
	r, err := nsq.NewReader(notifyTopic, notifyChannel)
	if err != nil {
		log.Fatal(err)
	}
	max, _ := strconv.ParseInt(maxInFlight, 10, 32)
	r.SetMaxInFlight(int(max))
	for i := 0; i < int(max); i++ {
		r.AddHandler(nt)
	}
	lookupdlist := strings.Split(lookupdAddresses, ",")
	for _, addr := range lookupdlist {
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
}
