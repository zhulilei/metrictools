package main

import (
	"github.com/datastream/metrictools/types"
	"io"
	"log"
	"net/http"
	"strconv"
)

func alarm_controller(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	w.WriteHeader(http.StatusOK)
	a_r := &types.Alarm{
		Exp: req.FormValue("metric"),
		V: atof64(req.FormValue("value")),
		T: atoi(req.FormValue("statistic_type")),
		J: atoi(req.FormValue("trigger_type")),
		P: atoi(req.FormValue("period")),
	}
	a_r2 := &types.Alarm{
		Exp: req.FormValue("metric_b"),
		V: atof64(req.FormValue("value_b")),
		T: atoi(req.FormValue("statistic_type_b")),
		J: atoi(req.FormValue("trigger_type_b")),
		P: atoi(req.FormValue("period_b")),
	}

	log.Println(a_r, a_r2)
	//r_type := tologic(req.FormValue("r_type"))

	session := mogo.session.Clone()
	defer session.Close()
	//	var query []types.Host
	//	var json string
	io.WriteString(w, "Add metric")
}

func tologic(s string) int {
	switch s {
	case "avg":
		return types.AVG
	case "sum":
		return types.SUM
	case "max":
		return types.MAX
	case "min":
		return types.MIN
	}
	return types.EXP
}

func atof64(s string) float64 {
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

func atoi(s string) int {
	to, err := strconv.Atoi(s)
	if err != nil {
		return -1
	}
	return to
}
