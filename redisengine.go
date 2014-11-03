package metrictools

import (
	"github.com/fzzy/radix/redis"
	"io"
	"log"
	"strconv"
)

type RedisEngine struct {
	*Setting
	ExitChannel chan int
	CmdChannel  chan interface{}
}

func (m *RedisEngine) Do(cmd string, args ...interface{}) *redis.Reply {
	req := Request{
		Cmd:          cmd,
		Args:         args,
		ReplyChannel: make(chan interface{}),
	}
	m.CmdChannel <- req
	response := <-req.ReplyChannel
	reply := response.(*redis.Reply)
	return reply
}

func (m *RedisEngine) RunTask() {
	for {
		select {
		case <-m.ExitChannel:
			return
		default:
			if err := m.commonLoop(); err == nil {
				return
			}
		}
	}
}

func (m *RedisEngine) commonLoop() error {
	client, err := redis.Dial(m.Network, m.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
		return err
	}
	for {
		select {
		case <-m.ExitChannel:
			return err
		case cmd := <-m.CmdChannel:
			if request, ok := cmd.(Request); ok {
				reply := client.Cmd(request.Cmd, request.Args...)
				err = reply.Err
				request.ReplyChannel <- reply
			} else if requests, ok := cmd.([]Request); ok {
				for _, request := range requests {
					client.Append(request.Cmd, request.Args...)
				}
				for _, request := range requests {
					reply := client.GetReply()
					err = reply.Err
					request.ReplyChannel <- reply
				}
			}
			if err != nil {
				if err != io.EOF {
					break
				}
				if client.Cmd("GET", "test").Err != nil {
					break
				}
			}
		}
	}
	defer client.Close()
	return err
}

func (m *RedisEngine) Stop() {
	close(m.ExitChannel)
}

// SetAdd define add a item into set
func (m *RedisEngine) SetAdd(set string, key string) error {
	return m.Do("SADD", set, key).Err
}

// SetDelete define remove a item from set
func (m *RedisEngine) SetDelete(set string, key string) error {
	return m.Do("SREM", set, key).Err
}

// GetSet define get all item from set
func (m *RedisEngine) GetSet(name string) ([]string, error) {
	return m.Do("SMEMBERS", name).List()
}

// DeleteData define remove data record from engine
func (m *RedisEngine) DeleteData(keys ...interface{}) error {
	return m.Do("DEL", keys...).Err
}

// GetValues define get archiveddata
func (m *RedisEngine) GetValues(keys ...interface{}) ([]string, error) {
	return m.Do("MGET", keys...).List()
}

// AppendKeyValue define append data to the key
func (m *RedisEngine) AppendKeyValue(key string, value interface{}) error {
	return m.Do("APPEND", key, value).Err
}

// SetKeyValue define set key's value
func (m *RedisEngine) SetKeyValue(key string, value interface{}) error {
	return m.Do("SET", key, value).Err
}

// SetTTL define set key's ttl
func (m *RedisEngine) SetTTL(key string, ttl int64) error {
	return m.Do("EXPIRE", key, ttl).Err
}

// SetAttr define set key's attr
func (m *RedisEngine) SetAttr(key string, attr string, value interface{}) error {
	return m.Do("HSET", key, attr, value).Err
}

func (m *RedisEngine) GetMetric(name string) (Metric, error) {
	info, err := m.Do("HMGET", name, "timestamp", "value", "atime", "rate_value", "ttl", "type").List()
	var metric Metric
	if err == nil {
		metric.Name = name
		metric.Mtype = info[5]
		metric.LastTimestamp, _ = strconv.ParseInt(info[0], 0, 64)
		metric.LastValue, _ = strconv.ParseFloat(info[1], 64)
		metric.ArchiveTime, _ = strconv.ParseInt(info[2], 0, 64)
		metric.RateValue, _ = strconv.ParseFloat(info[3], 64)
		metric.TTL, _ = strconv.ParseInt(info[4], 0, 64)
	}
	return metric, err
}

func (m *RedisEngine) SaveMetric(metric Metric) error {
	return m.Do("HMSET", metric.Name, "timestamp", metric.LastTimestamp, "value", metric.LastValue, "atime", metric.ArchiveTime, "rate_value", metric.RateValue, "ttl", metric.TTL, "type", metric.Mtype).Err
}

func (m *RedisEngine) GetUser(name string) (User, error) {
	userinfo, err := m.Do("HMGET", "user:"+name, "password", "group", "role", "permission").List()
	var u User
	if err == nil {
		u.Name = name
		u.Password = userinfo[0]
		u.Group = userinfo[1]
		u.Role = userinfo[2]
		u.Permission = userinfo[3]
	}
	return u, err
}

func (m *RedisEngine) GetToken(accessKey string) (AccessToken, error) {
	userInfo, err := m.Do("HGET", "access_key:"+accessKey, "user", "secretkey", "permission").List()
	var t AccessToken
	if err == nil {
		t.Name = accessKey
		t.UserName = userInfo[0]
		t.SecretKey = userInfo[1]
		t.Permission = userInfo[2]
	}
	return t, err
}

func (m *RedisEngine) GetNotifyAction(name string) (NotifyAction, error) {
	actionInfo, err := m.Do("HMGET", "action:"+name, "uri", "updated_time", "repeat", "count").List()
	var action NotifyAction
	if err == nil {
		action.Name = name
		action.Uri = actionInfo[0]
		action.UpdateTime, _ = strconv.ParseInt(actionInfo[1], 0, 64)
		action.Repeat, _ = strconv.Atoi(actionInfo[2])
		action.Count, _ = strconv.Atoi(actionInfo[3])
	}
	return action, err
}
func (m *RedisEngine) SaveNotifyAction(notifyAction NotifyAction) error {
	return m.Do("HMSET", "action:"+notifyAction.Name, "uri", notifyAction.Uri, "update_time", notifyAction.UpdateTime, "repeat", notifyAction.Repeat, "count", notifyAction.Count).Err
}

func (m *RedisEngine) GetTrigger(name string) (Trigger, error) {
	triggerInfo, err := m.Do("HMGET", "trigger:"+name, "owner", "is_expression", "last").List()
	var trigger Trigger
	if err == nil {
		trigger.Name = name
		trigger.Owner = triggerInfo[0]
		trigger.IsExpression, _ = strconv.ParseBool(triggerInfo[1])
		trigger.LastTime, _ = strconv.ParseInt(triggerInfo[2], 0, 64)
	}
	return trigger, err
}

func (m *RedisEngine) SaveTrigger(trigger Trigger) error {
	return m.Do("HMSET", "trigger:"+trigger.Name, "owner", trigger.Owner, "is_expression", trigger.IsExpression, "last", trigger.LastTime).Err
}
