package main

import (
	"log"
	"net/http"
	"strconv"
	"strings"
)

func MetricHandler(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=\"utf-8\"")
	metrics := req.FormValue("metrics") // all
	starttime := req.FormValue("starttime")
	endtime := req.FormValue("endtime")
	start := gettime(starttime)
	end := gettime(endtime)
	if !checktime(start, end) {
		start = end - 3600*3
	}

	metric_list := strings.Split(metrics, ",")
	session := db_session.Clone()
	defer session.Close()
	record_list := make(map[string][]interface{})
	redis_con := redis_pool.Get()
	for _, v := range metric_list {
		metric_data, err := redis_con.Do("ZRANGEBYSCORE",
			"archive:" + v, start, end)
		if err != nil {
			log.Println(err)
			continue
		}
		kv := gen_keyvalue(metric_data)
		record_list[v] = kv
	}
	w.Write(gen_json(record_list))
}

func gen_keyvalue(data interface{}) []interface{} {
	metric_data, ok := data.([][]byte)
	if !ok {
		return nil
	}
	var rst []interface{}
	for _, v := range metric_data {
		kv := strings.Split(string(v), ":")
		if len(kv) != 2 {
			continue
		}
		t, _ := strconv.ParseInt(kv[0], 10, 64)
		v, _ := strconv.ParseFloat(kv[1], 64)
		rst = append(rst, []interface{}{t, v})
	}
	return rst
}
