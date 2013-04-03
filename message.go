package metrictools

import (
	"encoding/json"
	"errors"
	"github.com/datastream/nsq/nsq"
	"github.com/garyburd/redigo/redis"
	"labix.org/v2/mgo"
	"log"
)

type Message struct {
	*nsq.Message
	ResponseChannel chan *nsq.FinishedMessage
}

type MsgDeliver struct {
	MessageChan    chan *Message
	MSession       *mgo.Session
	DBName         string
	RedisPool      *redis.Pool
	VerboseLogging bool
}

func (this *MsgDeliver) ParseJSON(c CollectdJSON) []*Record {
	keys := c.GenNames()
	var msgs []*Record
	for i := range c.Values {
		msg := &Record{
			Host:      c.Host,
			Key:       c.Host + "_" + keys[i],
			Value:     c.Values[i],
			Timestamp: int64(c.TimeStamp),
			TTL:       int(c.Interval) * 3 / 2,
			DSType:    c.DSTypes[i],
			Interval:  c.Interval,
		}
		msgs = append(msgs, msg)
	}
	return msgs
}

func (this *MsgDeliver) HandleMessage(m *nsq.Message, r chan *nsq.FinishedMessage) {
	this.MessageChan <- &Message{m, r}
}

func (this *MsgDeliver) ProcessData(collection string) {
	for {
		m := <-this.MessageChan
		go this.insert_data(collection, m)
	}
}

func (this *MsgDeliver) insert_data(collection string, m *Message) {
	var err error
	var c []CollectdJSON
	if err = json.Unmarshal(m.Body, &c); err != nil {
		m.ResponseChannel <- &nsq.FinishedMessage{
			m.Id, 0, true}
		log.Println(err)
		return
	}
	if this.VerboseLogging {
		log.Println("RAW JSON String: ", string(m.Body))
		log.Println("JSON SIZE: ", len(c))
	}
	stat := true
	for _, v := range c {
		if len(v.Values) != len(v.DSNames) {
			continue
		}
		if len(v.Values) != len(v.DSTypes) {
			continue
		}
		msgs := this.ParseJSON(v)
		if err := this.PersistData(msgs, collection); err != nil {
			stat = false
			break
		}
	}
	m.ResponseChannel <- &nsq.FinishedMessage{m.Id, 0, stat}
}

func (this *MsgDeliver) PersistData(msgs []*Record, collection string) error {
	redis_con := this.RedisPool.Get()
	session := this.MSession.Copy()
	defer session.Close()
	var err error
	for _, msg := range msgs {
		var new_value float64
		if msg.DSType == "counter" || msg.DSType == "derive" {
			new_value, err = this.get_new_value(redis_con, msg)
		}
		if err != nil && err.Error() == "ignore" {
			continue
		}
		if err != nil {
			log.Println("fail to get new value", err)
			return err
		}
		redis_con.Do("SADD", msg.Host, msg.Key)
		redis_con.Do("SET", msg.Key, new_value)
		v := &KeyValue{
			Timestamp: msg.Timestamp,
			Value:     msg.Value,
		}
		var body []byte
		if body, err = json.Marshal(v); err == nil {
			redis_con.Do("SET", "raw_"+msg.Key, body)
		}
		err = session.DB(this.DBName).C(collection).Insert(msg)
		if err != nil {
			if err.(*mgo.LastError).Code == 11000 {
				continue
			}
			log.Println("fail to insert mongo", err)
			break
		}
	}
	return err
}

func (this *MsgDeliver) get_new_value(redis_con redis.Conn, msg *Record) (float64, error) {
	var value float64
	v, err := redis_con.Do("GET", "raw_"+msg.Key)
	if err != nil {
		return 0, err
	}
	if v == nil {
		return msg.Value, nil
	}
	var tv KeyValue
	if err = json.Unmarshal(v.([]byte), &tv); err == nil {
		if tv.Timestamp == msg.Timestamp {
			err = errors.New("ignore")
		}
		if tv.Timestamp < msg.Timestamp {
			value = (msg.Value - tv.Value) /
				float64(msg.Timestamp-tv.Timestamp)
		}
		if tv.Timestamp > msg.Timestamp {
			value = (msg.Value - tv.Value) /
				float64(tv.Timestamp-msg.Timestamp)
		}
		if value < 0 {
			value = msg.Value
		}
	} else {
		value = msg.Value
	}
	return value, nil
}
