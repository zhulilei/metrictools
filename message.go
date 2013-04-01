package metrictools

import (
	"encoding/json"
	"github.com/datastream/nsq/nsq"
	"github.com/garyburd/redigo/redis"
	"labix.org/v2/mgo"
	"log"
	"strconv"
)

type NSQMsg struct {
	Body []byte
	Stat chan error
}

type Msg struct {
	Record
	Host string
	TTL  int
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
		key := c.Host + "_" + keys[i]
		msg := Msg{
			Host: c.Host,
			Record: Record{
				K: "raw_" + key,
				T: int64(c.TimeStamp),
			},
			TTL: int(c.Interval) * 3 / 2,
		}
		new_value := this.GetNewValue(msg.K, i, c)
		msg.V = c.Values[i]
		this.RedisInsertChan <- msg
		msg.K = key
		msg.V = new_value
		this.RedisInsertChan <- msg
		msgs = append(msgs, msg)
	}
	return msgs
}

func (this *MsgDeliver) GetNewValue(key string, i int, c CollectdJSON) float64 {
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

func (this *MsgDeliver) InsertDB(collection string) {
	session := this.MSession.Copy()
	defer session.Close()
	var err error
	for {
		var msgs []Msg
		var nsqmsg NSQMsg
		nsqmsg = <-this.MessageChan
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
			msgs = append(msgs, this.ParseJSON(v)...)
		}
		for _, msg := range msgs {
			err = session.DB(this.DBName).
				C(collection).
				Insert(msg.Record)
			if err != nil {
				if err.(*mgo.LastError).Code == 11000 {
					err = nil
				}
				break
			}
		}
		nsqmsg.Stat <- err
	}
}
