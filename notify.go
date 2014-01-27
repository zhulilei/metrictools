package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/garyburd/redigo/redis"
	"log"
	"net/smtp"
	"strings"
	"time"
)

// Notify define a notify task
type Notify struct {
	*Setting
	*redis.Pool
	reader      *nsq.Reader
	exitChannel chan int
	msgChannel  chan *Message
}

func (m *Notify) Run() error {
	var err error
	m.reader, err = nsq.NewReader(m.NotifyTopic, m.NotifyChannel)
	if err != nil {
		return err
	}
	m.reader.SetMaxInFlight(m.MaxInFlight)
	for i := 0; i < m.MaxInFlight; i++ {
		m.reader.AddHandler(m)
	}
	for _, addr := range m.LookupdAddresses {
		err = m.reader.ConnectToLookupd(addr)
		if err != nil {
			return err
		}
	}
	dial := func() (redis.Conn, error) {
		c, err := redis.Dial("tcp", m.RedisServer)
		if err != nil {
			return nil, err
		}
		return c, err
	}
	m.Pool = redis.NewPool(dial, 3)
	go m.sendNotify()
	return err
}

func (m *Notify) Stop() {
	m.reader.Stop()
	close(m.exitChannel)
	m.Pool.Close()
}

// HandleMessage is Notify's nsq handle function
func (m *Notify) HandleMessage(msg *nsq.Message) error {
	var notifyMsg map[string]string
	var err error
	if err = json.Unmarshal([]byte(msg.Body), &notifyMsg); err == nil {
		message := &Message{
			Body:         notifyMsg,
			ErrorChannel: make(chan error),
		}
		m.msgChannel <- message
		return <-message.ErrorChannel
	}
	log.Println(err)
	return nil
}

func (m *Notify) sendNotify() {
	con := m.Get()
	defer con.Close()
	var err error
	for {
		select {
		case <-m.exitChannel:
			return
		case msg := <-m.msgChannel:
			notifyMsg, ok := msg.Body.(map[string]string)
			if !ok {
				fmt.Println("message error:", msg.Body)
				msg.ErrorChannel <- nil
				continue
			}
			var keys []string
			keys, err = redis.Strings(con.Do("SMEMBERS", notifyMsg["trigger_exp"]+":actions"))
			if err != nil {
				if err == redis.ErrNil {
					log.Println("no action for", notifyMsg["trigger_exp"])
				} else {
					con.Close()
					con = m.Get()
				}
				msg.ErrorChannel <- nil
				continue
			}
			for _, v := range keys {
				var action NotifyAction
				var values []interface{}
				values, err = redis.Values(con.Do("HMGET", v, "uri", "updated_time", "repeat", "count"))
				if err != nil {
					log.Println("failed to get ", v)
					break
				}
				_, err = redis.Scan(values, &action.Uri, &action.UpdateTime, &action.Repeat, &action.Count)
				if err != nil {
					continue
				}
				n := time.Now().Unix()
				if ((n - action.UpdateTime) < 600) && (action.Repeat >= action.Count) {
					continue
				}
				uri := strings.Split(action.Uri, ":")
				switch uri[0] {
				case "mailto":
					if err = sendNotifyMail(notifyMsg["trigger_exp"], notifyMsg["time"]+"\n"+notifyMsg["event"]+"\n"+notifyMsg["url"], m.NotifyEmailAddress, []string{uri[1]}); err != nil {
						log.Println("fail to sendnotifymail", err)
						break
					}
				default:
					log.Println(notifyMsg)
				}
				_, err = con.Do("HMSET", v, "updated_time", n, "count", action.Count+1)
				if err != nil {
					break
				}
			}
			if err != nil {
				con.Close()
				con = m.Get()
			}
			msg.ErrorChannel <- nil
		}
	}
}

func sendNotifyMail(title, body, from string, to []string) error {
	header := make(map[string]string)
	header["To"] = strings.Join(to, ", ")
	header["Subject"] = title
	header["MIME-Version"] = "1.0"
	header["Content-Type"] = "text/plain; charset=\"utf-8\""
	header["Content-Transfer-Encoding"] = "base64"
	var msg string
	for k, v := range header {
		msg += fmt.Sprintf("%s: %s\r\n", k, v)
	}
	msg += "\r\n" + base64.StdEncoding.EncodeToString([]byte(body))

	err := smtp.SendMail(
		"localhost:25",
		nil,
		from,
		to,
		[]byte(msg),
	)
	return err
}
