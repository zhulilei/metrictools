package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"github.com/bitly/go-nsq"
	"github.com/fzzy/radix/redis"
	"log"
	"net/smtp"
	"os"
	"strconv"
	"strings"
	"time"
	"../.."
)

// Notify define a notify task
type Notify struct {
	*metrictools.Setting
	consumer    *nsq.Consumer
	exitChannel chan int
	msgChannel  chan *metrictools.Message
}

func (m *Notify) Run() error {
	hostname, err := os.Hostname()
	if err != nil {
		return err
	}
	cfg := nsq.NewConfig()
	cfg.Set("user_agent", fmt.Sprintf("metric_notify/%s", hostname))
	cfg.Set("snappy", true)
	cfg.Set("max_in_flight", m.MaxInFlight)
	m.consumer, err = nsq.NewConsumer(m.NotifyTopic, m.NotifyChannel, cfg)
	if err != nil {
		return err
	}
	m.consumer.AddConcurrentHandlers(m, m.MaxInFlight)
	err = m.consumer.ConnectToNSQLookupds(m.LookupdAddresses)
	if err != nil {
		return err
	}
	go m.sendNotify()
	return err
}

func (m *Notify) Stop() {
	m.consumer.Stop()
	close(m.exitChannel)
}

// HandleMessage is Notify's nsq handle function
func (m *Notify) HandleMessage(msg *nsq.Message) error {
	var notifyMsg map[string]string
	var err error
	if err = json.Unmarshal([]byte(msg.Body), &notifyMsg); err == nil {
		message := &metrictools.Message{
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
	client, err := redis.Dial(m.Network, m.RedisServer)
	if err != nil {
		log.Println("redis connection err", err)
	}
	defer client.Close()
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
			reply := client.Cmd("SMEMBERS", notifyMsg["trigger_exp"]+":actions")
			if reply.Err != nil {
				client.Close()
				log.Println("redis connection close")
				client, _ = redis.Dial(m.Network, m.RedisServer)
				msg.ErrorChannel <- nil
				continue
			}
			keys, err = reply.List()
			if err != nil {
				log.Println("no action for", notifyMsg["trigger_exp"], err)
			}
			for _, v := range keys {
				var action metrictools.NotifyAction
				var values []string
				reply := client.Cmd("HMGET", v, "uri", "updated_time", "repeat", "count")
				if reply.Err != nil {
					log.Println("failed to get ", v)
					break
				}
				values, _ = reply.List()
				action.Uri = values[0]
				action.UpdateTime, _ = strconv.ParseInt(values[1], 0, 64)
				action.Repeat, _ = strconv.Atoi(values[2])
				action.Count, _ = strconv.Atoi(values[4])
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
				reply = client.Cmd("HMSET", v, "updated_time", n, "count", action.Count+1)
				if reply.Err != nil {
					err = reply.Err
					break
				}
			}
			if err != nil {
				client.Close()
				client, _ = redis.Dial(m.Network, m.RedisServer)
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
