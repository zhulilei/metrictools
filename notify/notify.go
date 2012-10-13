package notify

import (
	"github.com/datastream/metrictools/types"
	"log"
)

func Send(act []types.AlarmAction, level int, value float64) {
	for i := range act.Action {
		switch act.Action[i].T {
		case "phone":
			{
				log.Println(act[i].Exp)
			}
		case "email":
			{
				log.Println(act[i].Exp)
			}
		case "im":
			{
				log.Println(act[i].Exp)
			}
		case "mq":
			{
				log.Println(act[i].Exp)
			}
		}
	}
}
