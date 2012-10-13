package notify

import (
	"encoding/json"
	"github.com/datastream/metrictools/types"
	"log"
)

type Message struct {
	Product string //blog, photo
	Type    string //nginx_cpu, nginx_req, app_cpu
	Level   int    //0 ok, 1 warning, 2 error
}
type Notify struct {
	Alarmaction types.AlarmAction
	Level       int
	Value       float64
}

func Send(notify_chan chan *Notify, msg_chan chan []byte) {
	for {
		nt := <-notify_chan
		almaction := nt.Alarmaction
		for i := range almaction.Act {
			var ac types.Action
			if err := json.Unmarshal(almaction.Act[i], &ac); err != nil {
				log.Println("Action encode error", err)
				continue
			}
			switch ac.T {
			case "phone":
				{
					log.Println(almaction.Exp)
				}
			case "email":
				{
					log.Println(almaction.Exp)
				}
			case "im":
				{
					log.Println(almaction.Exp)
				}
			case "mq":
				{
					msg := &Message{
						Product: almaction.Pd,
						Type:    almaction.Type,
						Level:   almaction.Stat,
					}
					if body, err := json.Marshal(msg); err == nil {
						msg_chan <- body
					} else {
						log.Println("encode error: ", err)
					}
				}
			}
		}
	}
}
