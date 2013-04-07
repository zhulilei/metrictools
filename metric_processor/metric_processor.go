package main

import (
	metrictools "../"
	"encoding/json"
	"flag"
	"github.com/datastream/nsq/nsq"
	"github.com/garyburd/redigo/redis"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
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
	mongouri, _ := c.Global["mongodb"]
	dbname, _ := c.Global["dbname"]
	user, _ := c.Global["user"]
	password, _ := c.Global["password"]
	lookupd_addresses, _ := c.Global["lookupd_addresses"]
	nsqd_addr, _ := c.Global["nsqd_addr"]
	maxinflight, _ := c.Global["maxinflight"]
	redis_count, _ := c.Global["redis_conn_count"]
	metric_channel, _ := c.Metric["channel"]
	metric_topic, _ := c.Metric["topic"]
	trigger_collection, _ := c.Trigger["collection"]
	trigger_topic, _ := c.Trigger["topic"]
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
	r, err := nsq.NewReader(metric_topic, metric_channel)
	if err != nil {
		log.Fatal(err)
	}
	con_max, _ := strconv.Atoi(redis_count)
	if con_max == 0 {
		con_max = 1
	}
	for i := 0; i < con_max; i++ {
		go msg_deliver.Redis()
	}
	max, _ := strconv.ParseInt(maxinflight, 10, 32)
	r.SetMaxInFlight(int(max))
	r.AddAsyncHandler(&msg_deliver)
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
	go msg_deliver.ProcessData()
	go ScanTrigger(db_session, dbname, trigger_collection, w, trigger_topic)
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	r.Stop()
	w.Stop()
}

func ScanTrigger(msession *mgo.Session, dbname string, collection string, w *nsq.Writer, topic string) {
	session := msession.Copy()
	defer session.Close()
	ticker := time.Tick(time.Minute)
	for {
		now := time.Now().Unix()
		var triggers []metrictools.Trigger
		err := session.DB(dbname).C(collection).Find(bson.M{
			"last": bson.M{"$lt": now - 120}}).All(&triggers)
		if err == nil {
			var trigger_bodys [][]byte
			for i := range triggers {
				body, err := json.Marshal(triggers[i])
				if err == nil {
					trigger_bodys = append(trigger_bodys,
						body)
				}
			}
			cmd, err := nsq.MultiPublish(topic, trigger_bodys)
			if err == nil {
				w.Write(cmd)
			}
		}
		<-ticker
	}
}
