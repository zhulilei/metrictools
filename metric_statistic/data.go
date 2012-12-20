package main

import (
	"encoding/json"
	"github.com/datastream/cal"
	"github.com/datastream/metrictools"
	"github.com/datastream/metrictools/amqp"
	"github.com/datastream/metrictools/notify"
	"github.com/garyburd/redigo/redis"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"strconv"
	"time"
)

// dispath trigger message to 2 channels
func trigger_chan_dispatch(trigger_chan chan *amqp.Message, update_chan, calculate_chan chan string) {
	for {
		msg := <-trigger_chan
		go func() {
			update_chan <- msg.Content
			calculate_chan <- msg.Content
			msg.Done <- 1
		}()
	}
}

// calculate trigger
func calculate_trigger(pool *redis.Pool, db_session *mgo.Session, dbname string, cal_chan chan string, notify_chan chan *notify.Notify) {
	session := db_session.Clone()
	for {
		exp := <-cal_chan
		var trigger metrictools.Trigger
		err := session.DB(dbname).C("Trigger").Find(bson.M{"exp": exp}).One(&trigger)
		if err == nil {
			go period_calculate_task(trigger, pool)
			go period_statistic_task(trigger, pool, db_session, dbname, notify_chan)
		}
	}
}

// calculate trigger.exp
func period_calculate_task(trigger metrictools.Trigger, pool *redis.Pool) {
	metrics := cal.Parser(trigger.Exp)
	ticker := time.NewTicker(time.Minute * time.Duration(trigger.I))
	id := 0
	redis_con := pool.Get()
	for {
		rst, err := calculate_exp(redis_con, metrics, trigger.Exp)
		if err != nil {
			redis_con = pool.Get()
			log.Println(trigger.Exp, " calculate failed.", err)
		} else {
			_, err := redis_con.Do("SET", "period_calculate_task:"+trigger.Exp+":"+strconv.Itoa(id), rst)
			if err != nil {
				redis_con = pool.Get()
				redis_con.Do("SET", "period_calculate_task:"+trigger.Exp+":"+strconv.Itoa(id), rst)
			}
			redis_con.Do("EXPIRE", "period_calculate_task:"+trigger.Exp+":"+strconv.Itoa(id), trigger.P*60)
			id++
		}
		<-ticker.C
	}
}

func calculate_exp(redis_con redis.Conn, metrics []string, exp string) (float64, error) {
	k_v := make(map[string]interface{})
	for i := range metrics {
		if len(metrics[i]) > 0 {
			v, err := redis_con.Do("GET", metrics[i])
			if err == nil {
				var d float64
				var value []byte
				if v == nil {
					d, err = strconv.ParseFloat(metrics[i], 64)
				} else {
					value, _ = v.([]byte)
					d, err = strconv.ParseFloat(string(value), 64)
				}
				if err == nil {
					k_v[metrics[i]] = d
				} else {
					log.Println("failed to load value of", metrics[i])
				}
			} else {
				return 0, err
			}
		}
	}
	rst, err := cal.Cal(exp, k_v)
	return rst, err
}

//get statistic result
func period_statistic_task(trigger metrictools.Trigger, pool *redis.Pool, db_session *mgo.Session, dbname string, notify_chan chan *notify.Notify) {
	session := db_session.Clone()
	ticker2 := time.NewTicker(time.Minute * time.Duration(trigger.P))
	redis_con := pool.Get()
	for {
		key := "period_calculate_task:" + trigger.Exp + "*"
		values, err := get_redis_keys_values(redis_con, key)
		if err != nil {
			redis_con = pool.Get()
			values, _ = get_redis_keys_values(redis_con, key)
		}
		stat, value := check_value(trigger, values)
		var tg metrictools.Trigger
		err = session.DB(dbname).C("Trigger").Find(bson.M{"exp": trigger.Exp}).One(&tg)
		if err == nil {
			if stat > 0 || tg.Stat != stat {
				notify := &notify.Notify{
					Exp:   trigger.Exp,
					Level: stat,
					Value: value,
				}
				notify_chan <- notify
			}
		}
		<-ticker2.C
	}
}

// read keys and return keys' values
func get_redis_keys_values(redis_con redis.Conn, key string) ([]float64, error) {
	v, err := redis_con.Do("KEYS", key)
	if err != nil {
		return nil, err
	}
	keys, _ := v.([]interface{})
	var values []float64
	for i := range keys {
		key, _ := keys[i].([]byte)
		v, err := redis_con.Do("GET", string(key))
		if err == nil {
			if value, ok := v.([]byte); ok {
				d, e := strconv.ParseFloat(string(value), 64)
				if e == nil {
					values = append(values, d)
				} else {
					log.Println(string(value), " convert to float64 failed")
				}
			}
		} else {
			return nil, err
		}
	}
	return values, nil
}

//update all trigger last modify time
func update_all_trigger(db_session *mgo.Session, dbname string, update_chan chan string) {
	for {
		trigger := <-update_chan
		go update_trigger(db_session, dbname, trigger)
	}
}

// update last modify time
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

// update last modify time
func update_statistic(db_session *mgo.Session, dbname string, statistic string) {
	session := db_session.Copy()
	defer session.Close()
	ticker := time.NewTicker(time.Second * 55)
	for {
		now := time.Now().Unix()
		session.DB(dbname).C("Statistic").Update(bson.M{"exp": statistic}, bson.M{"last": now})
		<-ticker.C
	}
}

// pack notify message
func deliver_notify(notify_chan chan *notify.Notify, deliver_chan chan *amqp.Message, routing_key string) {
	for {
		notify := <-notify_chan
		if body, err := json.Marshal(notify); err == nil {
			msg := &amqp.Message{
				Content: string(body),
				Key:     routing_key,
				Done:    make(chan int),
			}
			// i don't care whether msg send success or not
			deliver_chan <- msg
		} else {
			log.Println("encode error: ", err)
		}
	}
}

// check trigger's statistic value
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

// calculate statistic expression
func calculate_statistic_exp(pool *redis.Pool, db_session *mgo.Session, dbname string, cal_chan chan *amqp.Message) {
	for {
		msg := <-cal_chan
		go period_calculate_statistic_task(pool, db_session, dbname, msg.Content)
		go update_statistic(db_session, dbname, msg.Content)
		msg.Done <- 1
	}
}

// do calculate statistic
func period_calculate_statistic_task(pool *redis.Pool, db_session *mgo.Session, dbname string, exp string) {
	session := db_session.Clone()
	var statistic_exp metrictools.StatisticExp
	err := session.DB(dbname).C("Statistic").Find(bson.M{"exp": exp}).One(&statistic_exp)
	if err != nil {
		return
	}
	metrics := cal.Parser(statistic_exp.Exp)
	ticker := time.NewTicker(time.Minute * time.Duration(statistic_exp.I))
	redis_con := pool.Get()
	for {
		rst, err := calculate_exp(redis_con, metrics, statistic_exp.Exp)
		if err != nil {
			redis_con = pool.Get()
			log.Println(statistic_exp.Exp, " calculate failed.", err)
		} else {
			record := metrictools.StatisticRecord{
				Nm: statistic_exp.Nm,
				V:  rst,
				Ts: time.Now().Unix(),
			}
			session.DB(dbname).C("StatisticRecord").Insert(record)
		}
		<-ticker.C
	}
}

// get average value for []float64
func Avg_value(r []float64) float64 {
	return Sum_value(r) / float64(len(r))
}

// get sum of []float64
func Sum_value(r []float64) float64 {
	var rst float64
	rst = r[0]
	for i := range r {
		rst += r[i]
	}
	return rst
}

// get max value in []float64
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

// get min value in []float64
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

// return trigger's stat, 0 is ok, 1 is warning, 2 is error
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
