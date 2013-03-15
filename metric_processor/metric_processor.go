package main

import (
	metrictools "../"
	"encoding/json"
	"flag"
	"github.com/bitly/nsq/nsq"
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

type CollectdJSON struct {
	Values         []float64 `json:"values"`
	DSTypes        []string  `json:"dstypes"`
	DSNames        []string  `json:"dsnames"`
	TimeStamp      float64   `json:"time"`
	Interval       float64   `json:"interval"`
	Host           string    `json:"host"`
	Plugin         string    `json:"plugin"`
	PluginInstance string    `json:"plugin_instance"`
	Type           string    `json:"type"`
	TypeInstance   string    `json:"type_instance"`
}

func (this *CollectdJSON) GenNames() []string {
	base := this.Plugin
	if len(this.PluginInstance) > 0 {
		base += "_" + this.PluginInstance
	}
	if len(this.Type) > 0 {
		base += "." + this.Type
	}
	if len(this.TypeInstance) > 0 {
		base += "." + this.TypeInstance
	}
	var rst []string
	if len(this.DSNames) > 1 {
		for _, v := range this.DSNames {
			rst = append(rst, base+"."+v)
		}
	} else {
		rst = append(rst, base)
	}
	return rst
}

type NSQMsg struct {
	Body []byte
	Stat chan error
}

type Msg struct {
	metrictools.Record
	Host           string
	TTL            int
	CollectionName string
}

type MsgDeliver struct {
	json_chan         chan NSQMsg
	command_chan      chan NSQMsg
	msession          *mgo.Session
	dbname            string
	redis_insert_chan chan Msg
	redis_query_chan  chan metrictools.RedisQuery
	redis_pool        *redis.Pool
}

func (this *MsgDeliver) ParseJSON(c CollectdJSON) []Msg {
	keys := c.GenNames()
	var msgs []Msg
	for i := range c.Values {
		var msg Msg
		msg.Host = c.Host
		msg.K = c.Host + keys[i]
		msg.T = int64(c.TimeStamp)
		msg.TTL = int(c.Interval) * 3 / 2
		msg.V = this.GetValue(c.Host+keys[i], i, c)
		msg.CollectionName = c.Plugin + strconv.Itoa(int(c.Interval))
		msgs = append(msgs, msg)
	}
	return msgs
}

func (this *MsgDeliver) ParseCommand(command string) []Msg {
	lines := strings.Split(strings.TrimSpace(command), "\n")
	var msgs []Msg
	for _, line := range lines {
		items := strings.Split(line, " ")
		if len(items) != 4 {
			log.Println("line error")
			continue
		}
		var msg Msg
		keys := strings.Split(items[1], "/")
		if len(keys) != 3 {
			log.Println("key error")
			continue
		}
		msg.Host = keys[0]
		plugin := strings.Split(keys[1], "-")[0]
		msg.K = keys[0] + strings.Replace(keys[1], "-", "_", -1) +
			"." + strings.Replace(keys[2], "-", "_", -1)
		t, _ := strconv.ParseFloat(items[2][9:], 64)
		msg.TTL = int(t)
		t_v := strings.Split(items[3], ":")
		if len(t_v) != 2 {
			log.Println("value error")
			continue
		}
		ts, _ := strconv.ParseFloat(t_v[0], 64)
		msg.T = int64(ts)
		msg.V, _ = strconv.ParseFloat(t_v[1], 64)
		msg.CollectionName = plugin + strconv.Itoa(int(t)*3/2)
		msgs = append(msgs, msg)
	}
	return msgs
}

func (this *MsgDeliver) GetValue(key string, i int, c CollectdJSON) float64 {
	var value float64
	switch c.DSTypes[i] {
	case "counter":
		fallthrough
	case "derive":
		old_value := this.get_old_value(key)
		if c.Values[i] < old_value {
			// can't get max size setting
			// current collectd's default setting is U
			value = 0
		} else {
			value = (c.Values[i] - old_value) / c.Interval
		}
	default:
		value = c.Values[i]
	}
	return value
}
func (this *MsgDeliver) get_old_value(key string) float64 {
	q := metrictools.RedisQuery{
		Key:   key,
		Value: make(chan float64),
	}
	this.redis_query_chan <- q
	return <-q.Value
}

func (this *MsgDeliver) HandleMessage(m *nsq.Message) error {
	msg := NSQMsg{
		Body: m.Body,
		Stat: make(chan error),
	}
	this.json_chan <- msg
	return <-msg.Stat
}

func (this *MsgDeliver) RDeliver() {
	redis_con := this.redis_pool.Get()
	for {
		msg := <-this.redis_insert_chan
		_, err := redis_con.Do("SET", msg.K, msg.V)
		if err != nil {
			redis_con = this.redis_pool.Get()
			redis_con.Do("SET", msg.K, msg.V)
		}
		redis_con.Do("EXPIRE", msg.K, msg.TTL)
		redis_con.Do("SADD", msg.Host, msg.K)
	}
}

func (this *MsgDeliver) RQuery() {
	redis_con := this.redis_pool.Get()
	for {
		q := <-this.redis_query_chan
		v, err := redis_con.Do("GET", q.Key)
		if err == nil {
			if v != nil {
				value, _ := v.([]byte)
				d, err := strconv.ParseFloat(string(value), 64)
				if err == nil {
					q.Value <- d
					continue
				}
			}
		}
		q.Value <- 0
	}
}

func (this *MsgDeliver) InsertDB() {
	session := this.msession.Copy()
	defer session.Close()
	var err error
	for {
		var msgs []Msg
		var nsqmsg NSQMsg
		select {
		case nsqmsg = <-this.json_chan:
			var c []CollectdJSON
			if err = json.Unmarshal(nsqmsg.Body, &c); err != nil {
				nsqmsg.Stat <- nil
				log.Println(err)
				continue
			}
			for _, v := range c {
				if len(v.Values) != len(v.DSNames) {
					continue
				}
				if len(v.Values) != len(v.DSTypes) {
					continue
				}
				msgs = this.ParseJSON(v)
			}
		case nsqmsg = <-this.command_chan:
			msgs = this.ParseCommand(string(nsqmsg.Body))
		}
		for _, msg := range msgs {
			err = session.DB(this.dbname).
				C(msg.CollectionName).
				Insert(msg.Record)
			if err != nil {
				break
			}
			this.redis_insert_chan <- msg
		}
		nsqmsg.Stat <- err
	}
}

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
	maxInFlight, _ := c.Global["MaxInFlight"]
	metric_channel, _ := c.Metric["channel"]
	metric_topic, _ := c.Metric["topic"]
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
	msg_deliver := MsgDeliver{
		json_chan:         make(chan NSQMsg),
		command_chan:      make(chan NSQMsg),
		msession:          db_session,
		dbname:            dbname,
		redis_insert_chan: make(chan Msg),
		redis_query_chan:  make(chan metrictools.RedisQuery),
		redis_pool:        redis_pool,
	}
	defer db_session.Close()
	r, err := nsq.NewReader(metric_topic, metric_channel)
	if err != nil {
		log.Fatal(err)
	}
	max, _ := strconv.ParseInt(maxInFlight, 10, 32)
	r.SetMaxInFlight(int(max))
	r.AddHandler(&msg_deliver)
	lookupdlist := strings.Split(lookupd_addresses, ",")
	w := metrictools.NewWriter()
	for _, addr := range lookupdlist {
		log.Printf("lookupd addr %s", addr)
		err := r.ConnectToLookupd(addr)
		if err != nil {
			log.Fatal(err)
		}
		w.ConnectToLookupd(addr)
	}
	go msg_deliver.InsertDB()
	go msg_deliver.RDeliver()
	go msg_deliver.RQuery()
	go ScanTrigger(db_session, dbname, w, trigger_topic)
	go BuildIndex(db_session, dbname)
	termchan := make(chan os.Signal, 1)
	signal.Notify(termchan, syscall.SIGINT, syscall.SIGTERM)
	<-termchan
	r.Stop()
}

func ScanTrigger(msession *mgo.Session, dbname string, w *metrictools.Writer, topic string) {
	session := msession.Copy()
	defer session.Close()
	ticker := time.Tick(time.Minute)
	for {
		now := time.Now().Unix()
		var triggers []metrictools.Trigger
		err := session.DB(dbname).C("triggers").Find(bson.M{
			"last": bson.M{"$lt": now - 120}}).All(&triggers)
		if err == nil {
			for i := range triggers {
				w.Write(topic, []byte(triggers[i].Exp))
			}
		}
		<-ticker
	}
}

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
						Key:        []string{"k", "v", "t"},
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
