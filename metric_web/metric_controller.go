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
			"archive:"+v, start, end)
		if err != nil {
			log.Println(err)
			continue
		}
		md, ok := metric_data.([][]byte)
		if !ok {
			return
		}
		var kv []interface{}
		for _, v := range md {
			t_v := strings.Split(string(v), ":")
			if len(kv) != 2 {
				continue
			}
			t, _ := strconv.ParseInt(t_v[0], 10, 64)
			v, _ := strconv.ParseFloat(t_v[1], 64)
			kv = append(kv, []interface{}{t, v})
		}
		record_list[v] = kv
	}
	w.Write(gen_json(record_list))
}
