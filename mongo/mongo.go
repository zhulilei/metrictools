package mongo

import (
	"github.com/datastream/metrictools/notify"
	"github.com/datastream/metrictools/types"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"log"
	"regexp"
	"strings"
	"time"
)

type Mongo struct {
	session  *mgo.Session
	mongouri string
	dbname   string
	user     string
	password string
	done     chan error
}

func NewMongo(mongouri, dbname, user, password string) *Mongo {
	this := &Mongo{
		mongouri: mongouri,
		dbname:   dbname,
		user:     user,
		password: password,
	}
	return this
}

func (this *Mongo) connect_mongodb() {
	var err error
	for {
		this.session, err = mgo.Dial(this.mongouri)
		if err != nil {
			time.Sleep(time.Second * 2)
			continue
		}
		if len(this.user) > 0 {
			err = this.session.DB(this.dbname).Login(this.user, this.password)
			if err != nil {
				time.Sleep(time.Second * 2)
				continue
			}
		}
		break
	}
}
func (this *Mongo) Insert_record(message_chan chan *types.Message) {
	this.connect_mongodb()
	go this.handle_insert(message_chan)
	for {
		<-this.done
		this.session.Refresh()
		go this.handle_insert(message_chan)
	}
}

func (this *Mongo) handle_insert(message_chan chan *types.Message) {
	session := this.session.Copy()
	defer session.Close()
	for {
		var err error
		msg := <-message_chan
		metrics := strings.Split(strings.TrimSpace(msg.Content), "\n")
		for i := range metrics {
			record := types.NewMetric(metrics[i])
			if record != nil {
				if rst, _ := regexp.MatchString("sd[a-z]{1,2}[0-9]{1,2}", record.Nm); rst && record.App == "disk" {
					continue
				}
				if rst, _ := regexp.MatchString("(eth|br|bond)[0-9]{1,2}", record.Nm); !rst && record.App == "interface" {
					continue
				}
				err = session.DB(this.dbname).C(record.App).Insert(record.Record)
				splitname := strings.Split(metrics[i], " ")
				host := &types.Host{
					Host:   record.Hs,
					Metric: splitname[0],
					Ttl:    -1,
				}
				err = session.DB(this.dbname).C("host_metric").Insert(host)

				if err != nil {
					if rst, _ := regexp.MatchString("dup", err.Error()); rst {
						err = nil
					} else {
						log.Println("mongodb insert failed", err)
						this.done <- err
						break
					}
				}
			} else {
				log.Println("metrics error:", msg.Content)
			}
		}
		if err != nil {
			go func() {
				message_chan <- msg
			}()
			break
		}
		msg.Done <- 1
	}
}
func (this *Mongo) Scan_record(message_chan chan *types.Message) {
	this.connect_mongodb()
	go this.handle_scan(message_chan)
	for {
		<-this.done
		this.session.Refresh()
		go this.handle_scan(message_chan)
	}
}

func (this *Mongo) handle_scan(message_chan chan *types.Message) {
	for {
		msg := <-message_chan
		metrics := strings.Split(strings.TrimSpace(msg.Content), "\n")
		var err error
		for i := range metrics {
			metric := strings.Split(metrics[i], " ")
			if len(metric) < 1 {
				continue
			}
			if len(strings.TrimSpace(metric[0])) < 1 {
				continue
			}
			err = this.check_metric(metric[0])
		}
		if err != nil {
			go func() {
				message_chan <- msg
			}()
			break
		}
		msg.Done <- 1
	}
}

func (this *Mongo) check_metric(m string) error {
	session := this.session.Copy()
	defer session.Close()
	var err error
	var alm []types.Alarm
	var alm_relation []types.AlarmRelation

	err = session.DB(this.dbname).C("Alarm").Find(bson.M{"M": m}).All(&alm)
	stat := 0
	if len(alm) < 1 {
		return nil
	} else {
		stat = this.check_value(alm[0])
	}

	err = session.DB(this.dbname).C("AlarmRelation").Find(bson.M{"M1": m}).All(&alm_relation)
	if err != nil {
		log.Println("mongodb find failed", err)
		this.done <- err
		return err
	}
	stat2 := stat
	for i := range alm_relation {
		var am []types.Alarm
		_ = session.DB(this.dbname).C("Alarm").Find(bson.M{"M": alm_relation[i].M2}).All(&am)
		switch alm_relation[i].R {
		case types.AND:
			{
				if len(am) > 0 {
					stat &= this.check_value(am[0])
				} else {
					stat = 0
				}
			}
		case types.OR:
			{
				if len(am) > 0 {
					stat |= this.check_value(am[0])
				} else {
					stat = 0
				}
			}
		case types.XOR:
			{
				if len(am) > 0 {
					stat ^= this.check_value(am[0])
				} else {
					stat = 0
				}
			}
		case types.DIV:
			{
				stat = this.check_relation(alm[0], alm_relation[i])
			}
		}
		go this.trigger(m, stat)
		stat = stat2
	}
	if len(alm_relation) == 0 {
		go this.trigger(m, stat)
	}
	return err
}

func (this *Mongo) trigger(metric string, stat int) {
	var am types.AlarmAction
	session := this.session.Copy()
	defer session.Close()
	err := session.DB(this.dbname).C("AlarmAction").Find(bson.M{"M": metric}).One(&am)
	if err == nil {
		if am.Stat != stat {
			go notify.Send(am.C, metric, stat)
			am.Count++
			_ = session.DB(this.dbname).C("AlarmAction").Update(bson.M{"m": metric}, bson.M{"stat": stat, "count": am.Count})
		}
	}
}

func (this *Mongo) get_values(metric string, interval int) []types.Record {
	session := this.session.Clone()
	defer session.Close()
	var result []types.Record
	end := time.Now().Unix()
	start := end - int64(60*interval)
	var hosts []types.Host
	_ = session.DB(this.dbname).C("host_metric").Find(bson.M{"metric": bson.M{"$regex": metric}}).All(&hosts)

	for i := range hosts {
		m := types.NewMetric(hosts[i].Metric)
		var tmp []types.Record
		err := session.DB(this.dbname).C(m.App).Find(bson.M{"hs": m.Hs, "rt": m.Rt, "nm": m.Nm, "ts": bson.M{"$gt": start, "$lt": end}}).All(&tmp)
		if err == nil {
			result = append(result, tmp...)
		}
	}
	return result
}

func (this *Mongo) check_value(v types.Alarm) int {
	result := this.get_values(v.M, v.P)
	if len(result) < 1 {
		return 0
	}
	switch v.T {
	case types.AVG:
		{
			return types.Judge_value(v, types.Avg_value(result))
		}
	case types.SUM:
		{
			return types.Judge_value(v, types.Sum_value(result))
		}
	case types.MAX:
		{
			return types.Judge_value(v, types.Max_value(result))
		}
	case types.MIN:
		{
			return types.Judge_value(v, types.Min_value(result))
		}
	}
	return 0
}

func (this *Mongo) check_relation(v types.Alarm, r types.AlarmRelation) int {
	result := this.get_values(v.M, v.P)
	result2 := this.get_values(r.M2, v.P)
	if len(result2) < 1 || len(result) < 1 {
		return 0
	}
	rst := types.Div_value(result, result2)
	switch v.T {
	case types.AVG:
		{
			return types.Judge_value(v, types.Avg_value(rst))
		}
	case types.SUM:
		{
			return types.Judge_value(v, types.Sum_value(rst))
		}
	case types.MAX:
		{
			return types.Judge_value(v, types.Max_value(rst))
		}
	case types.MIN:
		{
			return types.Judge_value(v, types.Min_value(rst))
		}
	}
	return 1
}
