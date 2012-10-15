package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
)

var (
	mongouri = flag.String("mongouri", "mongodb://myuser:mypass@localhost:27017/mydatabase", "MONGODB RUI")
	user     = flag.String("user", "admin", "mongodb user")
	password = flag.String("passwd", "admin", "mongodb password")
	dbname   = flag.String("db", "mydatabase", "mongodb database")
	port     = flag.String("port", "1234", "server listean")
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
	http.HandleFunc("/monitorapi/metric", metric_controller)
	http.HandleFunc("/monitorapi/host", host_controller)
	http.HandleFunc("/monitorapi/types", type_controller)
	http.HandleFunc("/monitorapi/relation", relation_controller)
	http.HandleFunc("/monitorapi/alarm", alarm_controller)

	mogo = NewMongo(*mongouri, *dbname, *user, *password)

	err := http.ListenAndServe(":"+*port, nil)
	if err != nil {
		log.Println(err)
	}
}
