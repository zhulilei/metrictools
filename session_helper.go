package main

import (
	"net/http"
)

func filterLogin(w http.ResponseWriter, r *http.Request) (bool, error) {
	_, err := sessionservice.Get(r, queryservice.SessionName)
	if err != nil {
		return false, err
	}
	return true, nil
}
