package notify

import (
	"encoding/json"
	"github.com/datastream/metrictools"
	"log"
	"time"
)

type Message struct {
	Product string //domain: www.163.com, blog.163.com
	Type    string //nginx_cpu, nginx_req, app_cpu
	Level   int    //0 ok, 1 warning, 2 error
	Ts     int64
}
type Notify struct {
	Info   metrictools.Alarm
	Action []metrictools.AlarmAction
	Level  int
	Value  float64
}

func (this *Notify) Send(msg_chan chan []byte, repeated bool) {
	alarm_info := this.Info
	for i := range this.Action {
		switch this.Action[i].T {
		case "phone":
			{
				log.Println(alarm_info.Exp)
			}
		case "email":
			{
				log.Println(alarm_info.Exp)
			}
		case "im":
			{
				log.Println(alarm_info.Exp)
			}
		case "mq":
			{
				msg := &Message{
					Product: alarm_info.Pd,
					Type:    alarm_info.Nm,
					Level:   this.Level,
					Ts:      time.Now().Unix(),
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
