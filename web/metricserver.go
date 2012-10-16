package main

import (
	"flag"
	"fmt"
	"github.com/kless/goconfig/config"
	"log"
	"net/http"
	"os"
)

var (
	conf_file = flag.String("conf", "metrictools.conf", "analyst config file")
)

const (
	METRIC = 1
	APP    = 2
)

var mogo *Mongo

func main() {
	flag.Parse()
	if len(flag.Args()) != 1 {
		fmt.Printf("Usage: %s n\n\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}
	c, err := config.ReadDefault(*conf_file)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	mongouri, _ := c.String("Generic", "mongodb")
	dbname, _ := c.String("Generic", "dbname")
	user, _ := c.String("Generic", "user")
	password, _ := c.String("Generic", "password")
	port, _ := c.String("web", "port")

	http.HandleFunc("/monitorapi/metric", metric_controller)
	http.HandleFunc("/monitorapi/host", host_controller)
	http.HandleFunc("/monitorapi/types", type_controller)
	http.HandleFunc("/monitorapi/relation", relation_controller)
	http.HandleFunc("/monitorapi/alarm", alarm_controller)

	mogo = NewMongo(mongouri, dbname, user, password)

	err = http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Println(err)
	}
}
