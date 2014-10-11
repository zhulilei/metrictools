package main

import (
	"encoding/base64"
	"github.com/datastream/aws"
	"log"
	"net/http"
	"strings"
)

func (q *WebService) basicAuth(r *http.Request) string {
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
	rst, err := q.engine.Do("int", "HGET", "user:"+user, password)
	i := rst.(int)
	if err != nil || i != 1 {
		log.Println("redis hget error", err)
		user = ""
	}
	return user
}

func (q *WebService) awsSignv4(r *http.Request) string {
	var user string
	s, auth, err := sign4.GetSignature(r)
	if err != nil {
		return user
	}
	rst, err := q.engine.Do("strings", "HGET", "access_key:"+s.AccessKey, "user", "secretkey")
	userinfo := rst.([]string)
	if len(userinfo) != 2 || err != nil {
		log.Println("redis hget error", err)
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

func (q *WebService) loginFilter(r *http.Request) string {
	authorizationHeader := r.Header.Get("authorization")
	idents := strings.Split(authorizationHeader, " ")
	if idents[0] == "Basic" {
		return q.basicAuth(r)
	}
	if idents[0] == "AWS4-HMAC-SHA256" {
		return q.awsSignv4(r)
	}
	return ""
}
