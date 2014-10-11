package metrictools

import (
	"github.com/fzzy/radix/redis"
	"log"
)

type RedisEngine struct {
	*Setting
	exitChannel chan int
	CmdChannel  chan interface{}
}

func (m *RedisEngine) Do(rtype string, cmd string, args ...interface{}) (interface{}, error) {
	req := Request{
		Cmd:          cmd,
		Args:         args,
		ReplyChannel: make(chan interface{}),
	}
	m.CmdChannel <- req
	response := <-req.ReplyChannel
	reply := response.(*redis.Reply)
	switch rtype {
	case "int":
		return reply.Int()
	case "int64":
		return reply.Int64()
	case "bool":
		return reply.Bool()
	case "string":
		return reply.Str()
	case "strings":
		return reply.List()
	default:
	}
	return reply, reply.Err
}

func (m *RedisEngine) Start() {
	m.exitChannel = make(chan int)
	m.CmdChannel = make(chan interface{})
	for {
		select {
		case <-m.exitChannel:
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
		case <-m.exitChannel:
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
	close(m.exitChannel)
}
