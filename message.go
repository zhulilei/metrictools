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

type MsgDeliver struct {
	MessageChan     chan *Message
	MSession        *mgo.Session
	DBName          string
	RedisInsertChan chan Record
	RedisQueryChan  chan RedisQuery
	RedisPool       *redis.Pool
	VerboseLogging  bool
}

func (this *MsgDeliver) ParseJSON(c CollectdJSON) []*Record {
	keys := c.GenNames()
	var msgs []*Record
	for i := range c.Values {
		key := c.Host + "_" + keys[i]
		msg := &Record{
			Host:      c.Host,
			Key:       "raw_" + key,
			Timestamp: int64(c.TimeStamp),
			TTL:       int(c.Interval) * 3 / 2,
		}
		new_value := this.GetNewValue(msg.Key, i, c)
		msg.Value = c.Values[i]
		if this.VerboseLogging {
			log.Println("RAW : ", msg.Key, msg.Value, msg.Timestamp)
		}
		this.RedisInsertChan <- *msg
		msg.Key = key
		msg.Value = new_value
		if this.VerboseLogging {
			log.Println("New : ", msg.Key, msg.Value, msg.Timestamp)
		}
		this.RedisInsertChan <- *msg
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
		Value: make(chan []byte),
	}
	this.RedisQueryChan <- q
	v := <-q.Value
	if v != nil {
		d, err := strconv.ParseFloat(string(v), 64)
		if err == nil {
			return d
		}
	}
	return 0
}

func (this *MsgDeliver) HandleMessage(m *nsq.Message, r chan *nsq.FinishedMessage) {
	this.MessageChan <- &Message{m, r}
}

func (this *MsgDeliver) RDeliver() {
	redis_con := this.RedisPool.Get()
	for {
		msg := <-this.RedisInsertChan
		_, err := redis_con.Do("SET", msg.Key, msg.Value)
		if err != nil {
			redis_con = this.RedisPool.Get()
			redis_con.Do("SET", msg.Key, msg.Value)
		}
		if msg.Key[:3] == "raw" {
			continue
		}
		redis_con.Do("EXPIRE", msg.Key, msg.TTL)
		redis_con.Do("SADD", msg.Host, msg.Key)
	}
}

func (this *MsgDeliver) RQuery() {
	redis_con := this.RedisPool.Get()
	for {
		q := <-this.RedisQueryChan
		v, err := redis_con.Do("GET", q.Key)
		if err == nil && v != nil {
			q.Value <- v.([]byte)
		} else {
			q.Value <- nil
		}
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
	var msgs []*Record
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
		if this.VerboseLogging {
			log.Println("RAW JSON: ", v)
		}
		msgs = append(msgs, this.ParseJSON(v)...)
	}
	for _, msg := range msgs {
		err = session.DB(this.DBName).
			C(collection).
			Insert(msg)
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
