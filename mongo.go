package metrictools

import (
	"labix.org/v2/mgo"
)

func NewMongo(mongouri, dbname, user, password string) *mgo.Session {
	session, err := mgo.Dial(mongouri)
	if err != nil {
		return nil
	}
	if len(user) > 0 {
		err = session.DB(dbname).Login(user, password)
		if err != nil {
			return nil
		}
	}
	return session
}
