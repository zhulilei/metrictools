package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
)

func alarm_controller(w http.ResponseWriter, req *http.Request) {
	body, err := ioutil.ReadAll(req.Body)
	if err != nil {
		log.Println(err)
	}
	w.Header().Set("Content-Type", "text/plain; charset=\"utf-8\"")
	var alm_req AlarmRequest
	if err = json.Unmarshal(body, &alm_req); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Deny"))
		return
	} else {
		w.WriteHeader(http.StatusOK)
	}
	session := mogo.session.Clone()
	defer session.Close()
	for i := range alm_req.Act {
		body, err := json.Marshal(alm_req.Act[i])
		if err != nil {
			alm_req.Almact.Act = append(alm_req.Almact.Act, body)
	 	}
	}
	alm_req.Almact.Exp = alm_req.Alm.Exp
	if len(alm_req.Almact.Act) < 1 {
		w.Write([]byte("Failed insert"))
		return
	}
	err = session.DB(mogo.dbname).C("Alarm").Insert(alm_req.Alm)
	err = session.DB(mogo.dbname).C("AlarmAction").Insert(alm_req.Almact)
	if err != nil {
		w.Write([]byte("Failed insert"))
	} else {
		w.Write([]byte("Add successful"))
	}
}
