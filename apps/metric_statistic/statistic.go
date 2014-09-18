package main

import (
	"../.."
	"errors"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/datastream/cal"
	"github.com/fzzy/radix/redis"
	"log"
	"os"
	"strconv"
	"time"
)

// StatisticTask define a statistic task
type StatisticTask struct {
	*metrictools.Setting
	client           *redis.Client
	producer         *nsq.Producer
	exitChannel      chan int
	statisticChannel chan string
}

func (m *StatisticTask) Run() error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	cfg := nsq.NewConfig()
	cfg.Set("user_agent", fmt.Sprintf("metric_statistic/%s", hostname))
	cfg.Set("snappy", true)
	cfg.Set("max_in_flight", m.MaxInFlight)
	m.producer, err = nsq.NewProducer(m.NsqdAddress, cfg)
	if err == nil {
		go m.ScanTrigger()
		go m.calculateTask()
	}
	return err
}

func (m *StatisticTask) Stop() {
	close(m.exitChannel)
	m.producer.Stop()
}

// ScanStattistic will find out all trigger which not updated in 60s
func (m *StatisticTask) ScanTrigger() {
	ticker := time.Tick(time.Second * 30)
	client, err := redis.Dial(m.Network, m.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
	}
	defer client.Close()
	for {
		select {
		case <-ticker:
			reply := client.Cmd("SMEMBERS", "triggers")
			if reply.Err != nil {
				client.Close()
				log.Println("redis connection close")
				client, _ = redis.Dial(m.Network, m.RedisServer)
				continue
			}
			keys, _ := reply.List()
			now := time.Now().Unix()
			for _, v := range keys {
				reply = client.Cmd("HGET", v, "last")
				if reply.Err != nil {
					client.Close()
					log.Println("redis connection close")
					client, _ = redis.Dial(m.Network, m.RedisServer)
					continue
				}
				last, err := reply.Int64()
				if err != nil {
					continue
				}
				if now-last < 61 {
					continue
				}
				m.statisticChannel <- v
			}
		case <-m.exitChannel:
			return
		}
	}
}

func (m *StatisticTask) calculateTask() {
	client, _ := redis.Dial(m.Network, m.RedisServer)
	defer client.Close()
	for {
		select {
		case <-m.exitChannel:
			return
		case name := <-m.statisticChannel:
			now := time.Now().Unix()
			if reply := client.Cmd("HSET", name, "last", now); reply.Err != nil {
				client.Close()
				client, _ = redis.Dial(m.Network, m.RedisServer)
				continue
			}
			reply := client.Cmd("HGET", name, "is_e")
			if reply.Err != nil {
				log.Println("get trigger failed:", name, reply.Err)
			}
			if stat, _ := reply.Bool(); stat {
				err := m.calculate(name, client)
				log.Println("calculate failed", err)
			}
			m.producer.Publish(m.SkylineTopic, []byte(name))
		}
	}
}

func (m *StatisticTask) calculate(exp string, client *redis.Client) error {
	expList := cal.Parser(exp)
	v, err := m.evalExp(expList, client)
	if err != nil {
		log.Println("calculate failed:", exp, err)
		return err
	}
	t := time.Now().Unix()
	body, err := metrictools.KeyValueEncode(t, v)
	if err != nil {
		log.Println("encode data failed:", err)
		return err
	}
	client.Append("APPEND", fmt.Sprintf("archive:%s:%d", exp, t/14400), body)
	// ttl ?
	client.Append("EXPIRE", fmt.Sprintf("archive:%s:%d", exp, t/14400), 90000)
	client.GetReply()
	reply := client.GetReply()
	if reply.Err != nil {
		return reply.Err
	}
	return nil
}

func (m *StatisticTask) evalExp(expList []string, client *redis.Client) (float64, error) {
	kv := make(map[string]interface{})
	exp := ""
	current := time.Now().Unix()
	for _, item := range expList {
		var v float64
		reply := client.Cmd("HMGET", item, "rate_value", "timestamp")
		if reply.Err != nil {
			return 0, reply.Err
		}
		values, err := reply.List()
		v, err = strconv.ParseFloat(item, 64)
		if err != nil {
			v, _ = strconv.ParseFloat(values[0], 64)
		}
		t, err := strconv.ParseInt(values[1], 0, 64)
		if err != nil {
			t = time.Now().Unix()
		}
		if (current - t) > 60 {
			return 0, errors.New(item + "'s data are too old")
		}
		if err != nil {
			return 0, err
		}
		kv[item] = v
		exp += item
	}
	rst, err := cal.Cal(exp, kv)
	return rst, err
}
