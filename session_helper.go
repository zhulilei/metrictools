package main

import (
	"net/http"
)

func (q *WebService) filterLogin(w http.ResponseWriter, r *http.Request) (bool, error) {
	_, err := sessionservice.Get(r, q.SessionName)
	if err != nil {
		return false, err
	}
	return true, nil
}
