package metrictools

import (
	"encoding/json"
	"github.com/datastream/nsq/nsq"
	"github.com/garyburd/redigo/redis"
	"labix.org/v2/mgo"
	"log"
	"strconv"
)

type Message struct {
	*nsq.Message
	ResponseChannel chan *nsq.FinishedMessage
}

type Msg struct {
	Record
	Host string
	TTL  int
}

type MsgDeliver struct {
	MessageChan     chan *Message
	MSession        *mgo.Session
	DBName          string
	RedisInsertChan chan *Msg
	RedisQueryChan  chan RedisQuery
	RedisPool       *redis.Pool
}

func (this *MsgDeliver) ParseJSON(c CollectdJSON) []*Msg {
	keys := c.GenNames()
	var msgs []*Msg
	for i := range c.Values {
		key := c.Host + "_" + keys[i]
		msg := &Msg{
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

func (this *MsgDeliver) HandleMessage(m *nsq.Message, r chan *nsq.FinishedMessage) {
	this.MessageChan <- &Message{m, r}
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
		if msg.K[:3] == "raw" {
			continue
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
	for {
		m := <-this.MessageChan
		go this.insert(collection, m)
	}
}
func (this *MsgDeliver) insert(collection string, m *Message) {
	var err error
	session := this.MSession.Copy()
	defer session.Close()
	var c []CollectdJSON
	var msgs []*Msg
	if err = json.Unmarshal(m.Body, &c); err != nil {
		m.ResponseChannel <- &nsq.FinishedMessage{
			m.Id, 0, true}
		log.Println(err)
		return
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
			} else {
				break
			}
		}
	}
	stat := true
	if err != nil {
		stat = false
	}
	m.ResponseChannel <- &nsq.FinishedMessage{
		m.Id, 0, stat}
}
