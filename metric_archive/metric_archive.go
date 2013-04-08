package main

import (
	metrictools "../"
	"flag"
	"github.com/garyburd/redigo/redis"
	"labix.org/v2/mgo"
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
	mongouri, _ := c.Global["mongodb"]
	dbname, _ := c.Global["dbname"]
	user, _ := c.Global["user"]
	password, _ := c.Global["password"]
	redis_count, _ := c.Global["redis_conn_count"]
	redis_server, _ := c.Redis["server"]
	redis_auth, _ := c.Redis["auth"]

	db_session, err := mgo.Dial(mongouri)
	if err != nil {
		log.Fatal(err)
	}
	if len(user) > 0 {
		err = db_session.DB(dbname).Login(user, password)
		if err != nil {
			log.Fatal(err)
		}
	}
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
	msg_deliver := metrictools.MsgDeliver{
		MessageChan:    make(chan *metrictools.Message),
		MSession:       db_session,
		DBName:         dbname,
		RedisPool:      redis_pool,
		RedisChan:      make(chan *metrictools.RedisOP),
		VerboseLogging: false,
	}
	defer db_session.Close()
	con_max, _ := strconv.Atoi(redis_count)
	if con_max == 0 {
		con_max = 1
	}
	for i := 0; i < con_max; i++ {
		go msg_deliver.Redis()
	}
	go msg_deliver.ExpireData()
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
}
