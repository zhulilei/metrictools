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
	userID, _ := base64.StdEncoding.DecodeString(idents[1])
	idents = strings.Split(string(userID), ":")
	if len(idents) != 2 {
		return user
	}
	password := idents[1]
	u, err := q.engine.GetUser(idents[0])
	if err != nil {
		user = ""
	}
	if u.Password != password {
		user = ""
	}
	return u.Name
}

func (q *WebService) awsSignv4(r *http.Request) string {
	var user string
	s, auth, err := sign4.GetSignature(r)
	if err != nil {
		return user
	}
	token, err := q.engine.GetToken(s.AccessKey)
	if err != nil {
		log.Println("redis hget error", err)
		return user
	}
	s.SecretKey = token.SecretKey
	s.SignRequest(r)
	authheader := r.Header.Get("authorization")
	if auth != authheader {
		return user
	}
	return token.UserName
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
