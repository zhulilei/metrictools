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
	"regexp"
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
	metric_collection, _ := c.Metric["collection"]
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
	go msg_deliver.Redis()
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
	go msg_deliver.ProcessData(metric_collection)
	go ScanTrigger(db_session, dbname, trigger_collection, w, trigger_topic)
	go BuildIndex(db_session, dbname)
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

// need todo
func BuildIndex(msession *mgo.Session, dbname string) {
	session := msession.Copy()
	defer session.Close()
	ticker := time.Tick(time.Second * 3600)
	for {
		clist, err := session.DB(dbname).CollectionNames()
		if err != nil {
			time.Sleep(time.Second * 10)
			session.Refresh()
		} else {
			for i := range clist {
				if rst, _ := regexp.MatchString(
					"(system|trigger)",
					clist[i]); !rst {
					index := mgo.Index{
						Key:        []string{"t", "k"},
						Unique:     true,
						DropDups:   true,
						Background: true,
						Sparse:     true,
					}
					go mkindex(session, dbname, clist[i], index)
				}
				if rst, _ := regexp.MatchString(
					"trigger",
					clist[i]); rst {
					index := mgo.Index{
						Key:        []string{"exp"},
						Unique:     true,
						DropDups:   true,
						Background: true,
						Sparse:     true,
					}
					go mkindex(session, dbname, clist[i], index)
				}
				if rst, _ := regexp.MatchString(
					"(system|trigger)",
					clist[i]); !rst {
					index := mgo.Index{
						Key:         []string{"t"},
						Background:  true,
						Sparse:      true,
						ExpireAfter: time.Hour * 24 * 30,
					}
					go mkindex(session, dbname, clist[i], index)
				}
			}
		}
		<-ticker
	}
}

func mkindex(session *mgo.Session, dbname string, collection string, index mgo.Index) {
	if err := session.DB(dbname).C(collection).EnsureIndex(index); err != nil {
		log.Println("make index error: ", err)
	}
}
