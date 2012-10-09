package main

import (
	"io"
	"net/http"
	"../types"
	"strconv"
)

func alarm_controller(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	w.WriteHeader(http.StatusOK)
	a_r := &types.Alarm{
		M : req.FormValue("metric"),
		V : atof64(req.FormValue("value")),
		T : atoi(req.FormValue("statistic_type")),
		J : atoi(req.FormValue("trigger_type")),
		P : atoi(req.FormValue("period")),
	}
	a_r2 := &types.Alarm{
		M : req.FormValue("metric_b"),
		V : atof64(req.FormValue("value_b")),
		T : atoi(req.FormValue("statistic_type_b")),
		J : atoi(req.FormValue("trigger_type_b")),
		P : atoi(req.FormValue("period_b")),
	}

	r_type := tologic(req.FormValue("r_type"))

	switch r_type {
	case types.AND:
		{
		}
	case types.OR:
		{
		}
	case types.XOR:
		{
		}
	case types.DIV:
		{
		}
	}

	session := mogo.session.Clone()
	defer session.Close()
	var query []types.Host
	var json string
	io.WriteString(w, "Add metric")
}

func tologic(s string) int {
	switch s {
	case "and":
		return types.AND
	case "or":
		return types.OR
	case "xor":
		return types.XOR
	case "div":
		return types.DIV
	}
	return 0
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
