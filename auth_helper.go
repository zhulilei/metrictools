package main

import (
	"encoding/base64"
	"github.com/datastream/aws"
	"github.com/garyburd/redigo/redis"
	"net/http"
	"strings"
)

func basicAuth(r *http.Request, con redis.Conn) string {
	var user string
	authorizationHeader := r.Header.Get("authorization")
	idents := strings.Split(authorizationHeader, " ")
	if len(idents) < 2 || idents[0] != "Basic" {
		return user
	}
	userId, _ := base64.StdEncoding.DecodeString(idents[1])
	idents = strings.Split(string(userId), ":")
	if len(idents) != 2 {
		return user
	}
	user = idents[0]
	password := idents[1]
	i, err := redis.Int(con.Do("HGET", "user:"+user, password))
	if err != nil || i != 1 {
		user = ""
	}
	return user
}

func awsSignv4(r *http.Request, con redis.Conn) string {
	var user string
	s, auth, err := sign4.GetSignature(r)
	if err != nil {
		return user
	}
	userinfo, _ := redis.Strings(con.Do("HMGET", "access_key:"+s.AccessKey, "user", "secretkey"))
	if len(userinfo) != 2 {
		return user
	}
	s.SecretKey = userinfo[1]
	s.SignRequest(r)
	authheader := r.Header.Get("authorization")
	if auth != authheader {
		return user
	}
	user = userinfo[0]
	return user
}

func loginFilter(r *http.Request, con redis.Conn) string {
	authorizationHeader := r.Header.Get("authorization")
	idents := strings.Split(authorizationHeader, " ")
	if idents[0] == "Basic" {
		return basicAuth(r, con)
	}
	if idents[0] == "AWS4-HMAC-SHA256" {
		return awsSignv4(r, con)
	}
	return ""
}
