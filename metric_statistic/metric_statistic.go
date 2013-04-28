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
	nsqd_addr, _ := c.Global["nsqd_addr"]
	maxInFlight, _ := c.Global["MaxInFlight"]
	trigger_channel, _ := c.Trigger["channel"]
	trigger_topic, _ := c.Trigger["topic"]
	notify_topic, _ := c.Notify["topic"]
	redis_server, _ := c.Redis["server"]
	redis_auth, _ := c.Redis["auth"]
	config_redis_server, _ := c.Redis["config_server"]
	config_redis_auth, _ := c.Redis["config_auth"]

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

	// redis
	config_redis_con := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", config_redis_server)
		if err != nil {
			return nil, err
		}
		if _, err := c.Do("AUTH", config_redis_auth); err != nil {
			c.Close()
			return nil, err
		}
		return c, err
	}
	config_redis_pool := redis.NewPool(config_redis_con, 3)
	defer config_redis_pool.Close()
	if config_redis_pool.Get() == nil {
		log.Fatal(err)
	}

	msg_deliver := &metrictools.MsgDeliver{
		RedisPool:      redis_pool,
		VerboseLogging: false,
	}
	r, err := nsq.NewReader(trigger_topic, trigger_channel)
	if err != nil {
		log.Fatal(err)
	}
	go metrictools.Redis(msg_deliver.RedisPool, msg_deliver.RedisChan)
	max, _ := strconv.ParseInt(maxInFlight, 10, 32)
	r.SetMaxInFlight(int(max))
	w := nsq.NewWriter(0)
	w.ConnectToNSQ(nsqd_addr)
	tt := &TriggerTask{
		writer:          w,
		MsgDeliver:      msg_deliver,
		notifyTopic:     notify_topic,
		ConfigRedisPool: config_redis_pool,
		ConfigChan:      make(chan *metrictools.RedisOP),
	}
	go metrictools.Redis(tt.ConfigRedisPool, tt.ConfigChan)
	for i := 0; i < int(max); i++ {
		r.AddHandler(tt)
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
	w.Stop()
}
