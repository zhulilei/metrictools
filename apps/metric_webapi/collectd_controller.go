package main

import (
	"fmt"
	"github.com/gin-gonic/gin"
	"io/ioutil"
	"log"
	"net/http"
)

func (m *WebService) Collectd(c *gin.Context) {
	c.Header("Content-Type", "application/json; charset=\"utf-8\"")
	c.Header("Access-Control-Allow-Methods", "POST")
	user := c.MustGet("user").(string)
	if len(user) == 0 {
		c.Header("WWW-Authenticate", "Basic realm=\"user/securt_token of your account\"")
		c.String(http.StatusUnauthorized, "bad user")
		return
	}
	r := c.Request
	body, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		log.Println("data error", body)
		c.String(http.StatusBadRequest, "failed to read body")
		return
	}
	err = m.producer.Publish(m.MetricTopic, []byte(fmt.Sprintf("%s|%s", user, body)))
	if err != nil {
		c.String(http.StatusInternalServerError, "insert failed")
		return
	}
}
