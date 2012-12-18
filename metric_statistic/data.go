package main

import (
	"github.com/datastream/cal"
	"github.com/datastream/metrictools"
	"github.com/datastream/metrictools/amqp"
	"github.com/garyburd/redigo/redis"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"regexp"
	"strconv"
	"time"
)

func ensure_index(db_session *mgo.Session, dbname string) {
	session := db_session.Copy()
	defer session.Close()
	ticker := time.NewTicker(time.Second * 3600)
	for {
		clist, err := session.DB(dbname).CollectionNames()
		if err != nil {
			time.Sleep(time.Second * 10)
			session.Refresh()
		} else {
			for i := range clist {
				var index mgo.Index
				if rst, _ := regexp.MatchString("(Trigger)", clist[i]); rst {
					index = mgo.Index{
						Key:        []string{"exp"},
						Unique:     true,
						DropDups:   true,
						Background: true,
						Sparse:     true,
					}
				}
				if len(index.Key) > 0 {
					if err = session.DB(dbname).C(clist[i]).EnsureIndex(index); err != nil {
						session.Refresh()
						log.Println("make index error: ", err)
					}
				}
			}
			<-ticker.C
		}
	}
}
func trigger_chan_dispatch(trigger_chan chan *amqp.Message, update_chan, subscribe_chan chan string) {
	for {
		msg := <-trigger_chan
		go func() {
			update_chan <- msg.Content
			subscribe_chan <- msg.Content
			msg.Done <- 1
		}()
	}
}
func cal_trigger(pool *redis.Pool, db_session *mgo.Session, dbname string, cal_chan chan string, deliver_chan chan *amqp.Message) {
	session := db_session.Clone()
	defer session.Close()
	for {
		exp := <-cal_chan
		var trigger metrictools.Trigger
		err := session.DB(dbname).C("Trigger").Find(bson.M{"exp": exp}).One(&trigger)
		if err == nil {
			go period_cal(trigger, pool)
			go exp_trigger(trigger, pool, deliver_chan)
		}
	}
}
func period_cal(trigger metrictools.Trigger, pool *redis.Pool) {
	metrics := cal.Parser(trigger.Exp)
	ticker := time.NewTicker(time.Minute * time.Duration(trigger.I))
	redis_con := pool.Get()
	id := 0
	for {
		<-ticker.C
		k_v := make(map[string]float32)
		for i := range metrics {
			if len(metrics[i]) > 1 {
				v, err := redis_con.Do("GET", metrics[i])
				if err == nil {
					if value, ok := v.([]byte); ok {
						d, _ := strconv.ParseFloat(string(value), 64)
						k_v[metrics[i]] = float32(d)
					}
				}
			}
		}
		rst, err := cal.Cal(trigger.Exp, k_v)
		if err != nil {
			log.Println(trigger.Exp, " calculate failed.", err)
		} else {
			redis_con.Send("SET", trigger.Exp+":"+strconv.Itoa(id), rst)
			redis_con.Send("EXPIRE", trigger.Exp+":"+strconv.Itoa(id), trigger.P*60)
			redis_con.Flush()
			id++
		}
	}
}
func exp_trigger(trigger metrictools.Trigger, pool *redis.Pool, deliver_chan chan *amqp.Message) {
	redis_con := pool.Get()
	ticker2 := time.NewTicker(time.Minute * time.Duration(trigger.P))
	for {
		<-ticker2.C
		redis_con.Send("KEYS", trigger.Exp+":*")
		redis_con.Flush()
		v, _ := redis_con.Receive()
		if keys, ok := v.([]string); ok {
			var values []float64
			for i := range keys {
				v, err := redis_con.Do("GET", keys[i])
				if err == nil {
					if value, ok := v.([]byte); ok {
						d, _ := strconv.ParseFloat(string(value), 64)
						values = append(values, d)
					}
				}
			}
			_, _ = check_value(trigger, values)
		}
	}
}
func update_all_trigger(db_session *mgo.Session, dbname string, update_chan chan string) {
	for {
		trigger := <-update_chan
		go update_trigger(db_session, dbname, trigger)
	}
}
func update_trigger(db_session *mgo.Session, dbname string, trigger string) {
	session := db_session.Copy()
	defer session.Close()
	ticker := time.NewTicker(time.Second * 55)
	for {
		now := time.Now().Unix()
		session.DB(dbname).C("Trigger").Update(bson.M{"exp": trigger}, bson.M{"last": now})
		<-ticker.C
	}
}
func do_nofiy(trigger metrictools.Trigger, stat int, value float64, notify_chan chan *amqp.Message, db_session *mgo.Session, dbname string) {
	session := db_session.Clone()
	defer session.Close()
	var alarm_actions []metrictools.AlarmAction
	err := session.DB(dbname).C("alarm_action").Find(bson.M{"exp": trigger.Exp}).One(&alarm_actions)
	if err == nil {
	}
}

func check_value(trigger metrictools.Trigger, data []float64) (int, float64) {
	var rst float64
	switch trigger.T {
	case metrictools.AVG:
		{
			rst = Avg_value(data)
			return Judge_value(trigger, rst), rst
		}
	case metrictools.SUM:
		{
			rst = Sum_value(data)
			return Judge_value(trigger, rst), rst
		}
	case metrictools.MAX:
		{
			rst = Max_value(data)
			return Judge_value(trigger, rst), rst
		}
	case metrictools.MIN:
		{
			rst = Min_value(data)
			return Judge_value(trigger, rst), rst
		}
	}
	return 0, rst
}

func Avg_value(r []float64) float64 {
	return Sum_value(r) / float64(len(r))
}

func Sum_value(r []float64) float64 {
	var rst float64
	rst = r[0]
	for i := range r {
		rst += r[i]
	}
	return rst
}

func Max_value(r []float64) float64 {
	var rst float64
	rst = r[0]
	for i := range r {
		if rst < r[i] {
			rst = r[i]
		}
	}
	return rst
}

func Min_value(r []float64) float64 {
	var rst float64
	rst = r[0]
	for i := range r {
		if rst > r[i] {
			rst = r[i]
		}
	}
	return rst
}
func Judge_value(S metrictools.Trigger, value float64) int {
	if len(S.V) != 2 {
		return 0
	}
	switch S.J {
	case metrictools.LESS:
		{
			if value < S.V[1] {
				return 2
			}
			if value < S.V[0] {
				return 1
			}
		}
	case metrictools.GREATER:
		{
			if value > S.V[1] {
				return 2
			}
			if value > S.V[0] {
				return 1
			}
		}
	}
	return 0
}
