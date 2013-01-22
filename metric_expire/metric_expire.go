package main

import (
	"flag"
	"github.com/datastream/metrictools"
	"github.com/kless/goconfig/config"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"os"
	"time"
)

var (
	conf_file = flag.String("conf", "metrictools.conf", "analyst config file")
)

func main() {
	flag.Parse()
	c, err := config.ReadDefault(*conf_file)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	mongouri, _ := c.String("Generic", "mongodb")
	dbname, _ := c.String("Generic", "dbname")
	user, _ := c.String("Generic", "user")
	password, _ := c.String("Generic", "password")
	// mongodb
	db_session := metrictools.NewMongo(mongouri, dbname, user, password)
	defer db_session.Close()
	if db_session == nil {
		log.Println("connect database error")
		os.Exit(1)
	}
	clean_old_data(db_session, dbname)
}

func get_collections(db_session *mgo.Session, dbname string) []string {
	session := db_session.Copy()
	defer session.Close()
	clist, err := session.DB(dbname).CollectionNames()
	if err != nil {
		log.Println("read error", err)
	}
	return clist
}

func clean_old_data(db_session *mgo.Session, dbname string) {
	ticker := time.NewTicker(time.Hour * 24)
	for {
		collections := get_collections(db_session, dbname)
		for _, c := range collections {
			go expire_data(db_session, dbname, c)
		}
		<-ticker.C
	}
}

func expire_data(db_session *mgo.Session, dbname string, collection string) {
	last := time.Now().Unix() - 3600*24*45
	session := db_session.Copy()
	defer session.Close()
	_ = session.DB(dbname).C(collection).Remove(bson.M{"ts": bson.M{"$lt": last}})
}
