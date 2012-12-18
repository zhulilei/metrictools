package main

import (
	"io"
	"net/http"
)

func exp_controller(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	metric_exp := req.FormValue("exp")
	starttime := req.FormValue("starttime")
	endtime := req.FormValue("endtime")
	start := gettime(starttime)
	end := gettime(endtime)
	if !checktime(start, end) {
		start = end - 360
	}

	session := db_session.Clone()
	defer session.Close()
	var json string
	if len(json) > 0 {
		w.WriteHeader(http.StatusOK)
		io.WriteString(w, "["+json[:len(json)-1]+"]")
	} else {
		w.WriteHeader(http.StatusInternalServerError)
		io.WriteString(w, "internal error "+metric_exp)
	}
}
