package notify

import (
	"github.com/datastream/metrictools/types"
	"log"
)

func Send(act []types.Action, metric string, value float64, level int) {
	for i := range act {
		switch act[i].T {
		case "phone":
			{
				log.Println(act[i].Nm)
			}
		case "email":
			{
				log.Println(act[i].Nm)
			}
		case "email":
			{
				log.Println(act[i].Nm)
			}
		case "mq":
			{
				log.Println(act[i].Nm)
			}
		}
	}
}
