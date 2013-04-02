package main

import (
	metrictools "../"
	"flag"
	"github.com/datastream/nsq/nsq"
	"github.com/garyburd/redigo/redis"
	"labix.org/v2/mgo"
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
	mongouri, _ := c.Global["mongodb"]
	dbname, _ := c.Global["dbname"]
	user, _ := c.Global["user"]
	password, _ := c.Global["password"]
	lookupd_addresses, _ := c.Global["lookupd_addresses"]
	nsqd_addr, _ := c.Global["nsqd_addr"]
	maxInFlight, _ := c.Global["MaxInFlight"]
	trigger_collection, _ := c.Trigger["collection"]
	trigger_channel, _ := c.Trigger["channel"]
	trigger_topic, _ := c.Trigger["topic"]
	statistic_collection, _ := c.Statistic["collection"]
	notify_topic, _ := c.Notify["topic"]
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
	msg_deliver := &metrictools.MsgDeliver{
		MessageChan:     make(chan *metrictools.Message),
		MSession:        db_session,
		DBName:          dbname,
		RedisInsertChan: make(chan *metrictools.Record),
		RedisQueryChan:  make(chan metrictools.RedisQuery),
		RedisPool:       redis_pool,
	}
	defer db_session.Close()
	r, err := nsq.NewReader(trigger_topic, trigger_channel)
	if err != nil {
		log.Fatal(err)
	}
	max, _ := strconv.ParseInt(maxInFlight, 10, 32)
	r.SetMaxInFlight(int(max))
	r.AddAsyncHandler(msg_deliver)
	lookupdlist := strings.Split(lookupd_addresses, ",")
	w := nsq.NewWriter()
	w.ConnectToNSQ(nsqd_addr)
	for _, addr := range lookupdlist {
		log.Printf("lookupd addr %s", addr)
		err := r.ConnectToLookupd(addr)
		if err != nil {
			log.Fatal(err)
		}
	}
	go msg_deliver.RQuery()
	go trigger_task(msg_deliver, w, notify_topic,
		trigger_collection, statistic_collection)
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	r.Stop()
	w.Stop()
}
