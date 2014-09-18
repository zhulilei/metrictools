package main

import (
	"strconv"
	"time"
)

func gettime(t string) int64 {
	now := time.Now().Unix()
	if len(t) == 0 {
		return now
	}
	rst, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		rst = now
	}
	if rst > now {
		rst = now
	}
	return rst
}
func checktime(start, end int64) bool {
	if start < end {
		return true
	}
	return false
}
