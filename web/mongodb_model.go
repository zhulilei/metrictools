package main

import (
	"labix.org/v2/mgo"
	"log"
)

type Mongo struct {
	session  *mgo.Session
	mongouri string
	dbname   string
	user     string
	password string
	done     chan error
}

func NewMongo(mongouri, dbname, user, password string) *Mongo {
	this := &Mongo{
		mongouri: mongouri,
		dbname:   dbname,
		user:     user,
		password: password,
	}
	var err error
	this.session, err = mgo.Dial(this.mongouri)
	if err != nil {
		log.Println(err)
	}
	if len(this.user) > 0 {
		err = this.session.DB(this.dbname).Login(this.user, this.password)
		if err != nil {
			log.Println(err)
		}
	}
	return this
}
