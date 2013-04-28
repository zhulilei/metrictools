package main

import (
	metrictools "../"
	"flag"
	"github.com/bitly/nsq/nsq"
	"github.com/garyburd/redigo/redis"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
)

var (
	conf_file = flag.String("conf", "metrictools.conf", "analyst config file")
)

func main() {
	flag.Parse()
	c, err := metrictools.ReadConfig(*conf_file)
	if err != nil {
		log.Fatal("config parse error", err)
	}
	lookupd_addresses, _ := c.Global["lookupd_addresses"]
	maxInFlight, _ := c.Global["MaxInFlight"]
	notify_channel, _ := c.Notify["channel"]
	notify_topic, _ := c.Notify["topic"]
	redis_server, _ := c.Redis["server"]
	redis_auth, _ := c.Redis["auth"]

	redis_con := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", redis_server)
		if err != nil {
			return nil, err
		}
		if _, err := c.Do("AUTH", redis_auth); err != nil {
			c.Close()
			return nil, err
		}
		return c, err
	}
	redis_pool := redis.NewPool(redis_con, 3)
	defer redis_pool.Close()
	if redis_pool.Get() == nil {
		log.Fatal(err)
	}
	msg_deliver := &metrictools.MsgDeliver{
		RedisPool:      redis_pool,
		VerboseLogging: false,
	}
	nt := &Notify{
		MsgDeliver: msg_deliver,
	}
	go metrictools.Redis(msg_deliver.RedisPool, msg_deliver.RedisChan)
	r, err := nsq.NewReader(notify_topic, notify_channel)
	if err != nil {
		log.Fatal(err)
	}
	max, _ := strconv.ParseInt(maxInFlight, 10, 32)
	r.SetMaxInFlight(int(max))
	for i := 0; i < int(max); i++ {
		r.AddHandler(nt)
	}
	lookupdlist := strings.Split(lookupd_addresses, ",")
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
