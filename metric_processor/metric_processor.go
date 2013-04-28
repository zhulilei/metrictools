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
	"time"
)

var (
	conf_file = flag.String("conf", "metrictools.json", "metrictools config file")
)

func main() {
	flag.Parse()
	c, err := metrictools.ReadConfig(*conf_file)
	if err != nil {
		log.Fatal("config parse error", err)
	}
	lookupd_addresses, _ := c.Global["lookupd_addresses"]
	nsqd_addr, _ := c.Global["nsqd_addr"]
	maxinflight, _ := c.Global["maxinflight"]
	redis_count, _ := c.Global["redis_conn_count"]
	metric_channel, _ := c.Metric["channel"]
	metric_topic, _ := c.Metric["topic"]
	trigger_topic, _ := c.Trigger["topic"]
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
		RedisChan:      make(chan *metrictools.RedisOP),
		VerboseLogging: false,
	}
	r, err := nsq.NewReader(metric_topic, metric_channel)
	if err != nil {
		log.Fatal(err)
	}
	con_max, _ := strconv.Atoi(redis_count)
	if con_max == 0 {
		con_max = 1
	}
	for i := 0; i < con_max; i++ {
		go metrictools.Redis(msg_deliver.RedisPool, msg_deliver.RedisChan)
	}
	max, _ := strconv.ParseInt(maxinflight, 10, 32)
	r.SetMaxInFlight(int(max))
	for i := 0; i < int(max); i++ {
		r.AddHandler(msg_deliver)
	}
	lookupdlist := strings.Split(lookupd_addresses, ",")
	w := nsq.NewWriter(0)
	w.ConnectToNSQ(nsqd_addr)
	for _, addr := range lookupdlist {
		log.Printf("lookupd addr %s", addr)
		err := r.ConnectToLookupd(addr)
		if err != nil {
			log.Fatal(err)
		}
	}
	confchan := make(chan *metrictools.RedisOP)
	go metrictools.Redis(config_redis_pool, confchan)
	go ScanTrigger(confchan, w, trigger_topic, nsqd_addr)
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	r.Stop()
	w.Stop()
}

func ScanTrigger(confchan chan *metrictools.RedisOP, w *nsq.Writer, topic string, addr string) {
	ticker := time.Tick(time.Minute)
	for {
		now := time.Now().Unix()
		v, err := metrictools.RedisDo(confchan, "KEYS", "trigger:*", nil)
		if err != nil {
			continue
		}
		if v != nil {
			for _, value := range v.([]interface{}) {
				last, err := metrictools.RedisDo(confchan,
					"HGET", string(value.([]byte)), "lastmodify")
				if err != nil {
					continue
				}
				if last != nil {
					d, _ := strconv.ParseInt(string(last.([]byte)), 10, 64)
					if now-d < 61 {
						continue
					}
				}
				_, _, err = w.Publish(topic, value.([]byte))
				if err != nil {
					w.ConnectToNSQ(addr)
				}
			}
		}
		<-ticker
	}
}
