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
	maxInFlight, _ := c["archivemaxinflight"]
	redisServer, _ := c["data_redis_server"]
	redisAuth, _ := c["data_redis_auth"]
	archiveChannel, _ := c["archive_channel"]
	archiveTopic, _ := c["archive_topic"]

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

	dr := &DataArchive{
		dataService: redisPool,
	}
	r, err := nsq.NewReader(archiveTopic, archiveChannel)
	if err != nil {
		log.Fatal(err)
	}
	max, _ := strconv.ParseInt(maxInFlight, 10, 32)
	r.SetMaxInFlight(int(max))
	for i := 0; i < int(max); i++ {
		r.AddHandler(dr)
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
}
