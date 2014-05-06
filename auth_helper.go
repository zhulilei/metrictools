package main

import (
	"encoding/base64"
	"github.com/garyburd/redigo/redis"
	"net/http"
	"strings"
)

func checkbasicauth(w http.ResponseWriter, r *http.Request, con redis.Conn) string {
	var user string
	auth := r.Header.Get("Authorization")
	idents := strings.Split(auth, " ")
	if len(idents) < 2 || idents[0] != "Basic" {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return user
	}
	userId, _ := base64.StdEncoding.DecodeString(idents[1])
	idents = strings.Split(string(userId), ":")
	if len(idents) != 2 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return user
	}
	user = idents[0]
	password := idents[1]
	i, err := redis.Int(con.Do("HMGET", "user:"+user, password))
	if err != nil || i != 1 {
		user = ""
	}
	return user
}
