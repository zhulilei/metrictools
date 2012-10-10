package notify

import (
	"github.com/datastream/metrictools/types"
	"log"
)

func Send(addr []types.Address, metric string, stat int) {
	for i := range addr {
		switch addr[i].T {
		case "phone":
			{
				log.Println(addr[i].ID)
			}
		case "email":
			{
				log.Println(addr[i].ID)
			}
		}
	}
}
