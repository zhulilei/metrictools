package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
)

func (m *WebService) Collectd(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	w.Header().Set("Access-Control-Allow-Methods", "POST")
	user := m.loginFilter(r)
	if len(user) == 0 {
		w.Header().Set("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		log.Println("data error", body)
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	err = m.producer.Publish(m.MetricTopic, []byte(fmt.Sprintf("%s|%s", user, body)))
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}
