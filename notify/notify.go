package notify

import (
	"github.com/datastream/metrictools/types"
	"log"
)

func Send(almaction []types.AlarmAction, level int, value float64) {
	for i := range almaction.Act {
		switch almaction.Act[i] {
		case "phone":
			{
				log.Println(almaction[i].Exp)
			}
		case "email":
			{
				log.Println(almaction[i].Exp)
			}
		case "im":
			{
				log.Println(almaction[i].Exp)
			}
		case "mq":
			{
				log.Println(almaction[i].Exp)
			}
		}
	}
}
