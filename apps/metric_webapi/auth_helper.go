package main

import (
	"encoding/base64"
	"github.com/datastream/aws"
	"github.com/fzzy/radix/redis"
	"log"
	"net/http"
	"strings"
)

func basicAuth(r *http.Request, client *redis.Client) string {
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
	i, err := client.Cmd("HGET", "user:"+user, password).Int()
	if err != nil || i != 1 {
		log.Println("redis hget error", err)
		user = ""
	}
	return user
}

func awsSignv4(r *http.Request, client *redis.Client) string {
	var user string
	s, auth, err := sign4.GetSignature(r)
	if err != nil {
		return user
	}
	userinfo, err := client.Cmd("HMGET", "access_key:"+s.AccessKey, "user", "secretkey").List()
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

func loginFilter(r *http.Request, client *redis.Client) string {
	authorizationHeader := r.Header.Get("authorization")
	idents := strings.Split(authorizationHeader, " ")
	if idents[0] == "Basic" {
		return basicAuth(r, client)
	}
	if idents[0] == "AWS4-HMAC-SHA256" {
		return awsSignv4(r, client)
	}
	return ""
}
