package main

import (
	metrictools "../"
	"encoding/base64"
	"encoding/json"
	"fmt"
	nsq "github.com/bitly/go-nsq"
	"github.com/garyburd/redigo/redis"
	"log"
	"net/smtp"
	"strings"
	"time"
)

type Notify struct {
	*redis.Pool
	EmailAddress string
}

func (this *Notify) HandleMessage(m *nsq.Message) error {
	var notify_msg map[string]string
	var err error
	if err = json.Unmarshal([]byte(m.Body), &notify_msg); err == nil {
		go this.SendNotify(notify_msg)
	}
	return err
}

func (this *Notify) SendNotify(notify_msg map[string]string) {
	config_con := this.Get()
	defer config_con.Close()
	keys, err := redis.Strings(config_con.Do("KEYS", "actions:"+notify_msg["trigger"]+":*"))
	if err != nil {
		log.Println("no action for", notify_msg["trigger"])
		return
	}
	for _, v := range keys {
		var action metrictools.NotifyAction
		values, err := redis.Values(config_con.Do("HMGET", v, "uri", "update_time", "repeat", "count"))
		if err != nil {
			log.Println("failed to get ", v)
			continue
		}
		_, err = redis.Scan(values, &action.Uri, &action.UpdateTime, &action.Repeat, &action.Count)
		if err != nil {
			continue
		}
		n := time.Now().Unix()
		if ((n-action.UpdateTime) < 600) && (action.Repeat >= action.Count) {
			continue
		}
		uri := strings.Split(action.Uri, ":")
		switch uri[0] {
		case "mailto":
			if err = SendNotifyMail(notify_msg["trigger"], notify_msg["time"]+"\n"+notify_msg["event"]+"\n"+notify_msg["url"], this.EmailAddress, []string{uri[1]}); err != nil {
				log.Println("fail to sendnotifymail",err)
			}
		default:
			log.Println(notify_msg)
		}
	}
}

func SendNotifyMail(title, body, from string, to []string) error {
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
