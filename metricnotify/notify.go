package main

import (
	metrictools "../"
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
	*redis.Pool
	EmailAddress string
}

// HandleMessage is Notify's nsq handle function
func (m *Notify) HandleMessage(msg *nsq.Message) error {
	var notifyMsg map[string]string
	var err error
	if err = json.Unmarshal([]byte(msg.Body), &notifyMsg); err == nil {
		go m.sendNotify(notifyMsg)
	}
	return err
}

func (m *Notify) sendNotify(notifyMsg map[string]string) {
	configCon := m.Get()
	defer configCon.Close()
	keys, err := redis.Strings(configCon.Do("SMEMBERS", notifyMsg["trigger"]+":actions"))
	if err != nil {
		log.Println("no action for", notifyMsg["trigger"])
		return
	}
	for _, v := range keys {
		var action metrictools.NotifyAction
		values, err := redis.Values(configCon.Do("HMGET", v, "uri", "update_time", "repeat", "count"))
		if err != nil {
			log.Println("failed to get ", v)
			continue
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
			if err = sendNotifyMail(notifyMsg["trigger_exp"], notifyMsg["time"]+"\n"+notifyMsg["event"]+"\n"+notifyMsg["url"], m.EmailAddress, []string{uri[1]}); err != nil {
				log.Println("fail to sendnotifymail", err)
			}
		default:
			log.Println(notifyMsg)
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
