package main

import (
	metrictools "../"
	"flag"
	"github.com/garyburd/redigo/redis"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
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
	redis_count, _ := c["redis_conn_count"]
	redis_server, _ := c["data_redis_server"]
	redis_auth, _ := c["data_redis_auth"]

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
	rs := &metrictools.RedisService{
		RedisPool: redis_pool,
		RedisChan: make(chan *metrictools.RedisOP),
	}
	dr := DataArchive{rs}
	con_max, _ := strconv.Atoi(redis_count)
	if con_max == 0 {
		con_max = 1
	}
	for i := 0; i < con_max; i++ {
		go rs.Run()
	}
	go dr.CompressData()
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
}
