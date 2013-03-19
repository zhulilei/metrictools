package metrictools

import (
	"encoding/json"
	"github.com/bitly/nsq/nsq"
	"github.com/garyburd/redigo/redis"
	"labix.org/v2/mgo"
	"log"
	"strconv"
	"strings"
)

type NSQMsg struct {
	Body []byte
	Stat chan error
}

type Msg struct {
	Record
	Host           string
	TTL            int
	CollectionName string
}

type MsgDeliver struct {
	MessageChan     chan NSQMsg
	MSession        *mgo.Session
	DBName          string
	RedisInsertChan chan Msg
	RedisQueryChan  chan RedisQuery
	RedisPool       *redis.Pool
}

func (this *MsgDeliver) ParseJSON(c CollectdJSON) []Msg {
	keys := c.GenNames()
	var msgs []Msg
	for i := range c.Values {
		var msg Msg
		msg.Host = c.Host
		msg.K = c.Host + ":" + keys[i]
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
		msg.K = msg.Host + ":" +
			strings.Replace(keys[1], "-", "_", -1) +
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
	q := RedisQuery{
		Key:   key,
		Value: make(chan float64),
	}
	this.RedisQueryChan <- q
	return <-q.Value
}

func (this *MsgDeliver) HandleMessage(m *nsq.Message) error {
	msg := NSQMsg{
		Body: m.Body,
		Stat: make(chan error),
	}
	this.MessageChan <- msg
	return <-msg.Stat
}

func (this *MsgDeliver) RDeliver() {
	redis_con := this.RedisPool.Get()
	for {
		msg := <-this.RedisInsertChan
		_, err := redis_con.Do("SET", msg.K, msg.V)
		if err != nil {
			redis_con = this.RedisPool.Get()
			redis_con.Do("SET", msg.K, msg.V)
		}
		redis_con.Do("EXPIRE", msg.K, msg.TTL)
		redis_con.Do("SADD", msg.Host, msg.K)
	}
}

func (this *MsgDeliver) RQuery() {
	redis_con := this.RedisPool.Get()
	for {
		q := <-this.RedisQueryChan
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
	session := this.MSession.Copy()
	defer session.Close()
	var err error
	for {
		var msgs []Msg
		var nsqmsg NSQMsg
		select {
		case nsqmsg = <-this.MessageChan:
			if string(nsqmsg.Body[:1]) == "P" {
				msgs = this.ParseCommand(string(nsqmsg.Body))
			} else {
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
			}
		}
		for _, msg := range msgs {
			err = session.DB(this.DBName).
				C(msg.CollectionName).
				Insert(msg.Record)
			if err != nil {
				break
			}
			this.RedisInsertChan <- msg
		}
		nsqmsg.Stat <- err
	}
}
