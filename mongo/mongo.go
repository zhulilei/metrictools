package mongo

import (
	"github.com/datastream/cal"
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
			var alm []types.Alarm
			err = this.session.DB(this.dbname).C("Alarm").Find(bson.M{"exp": bson.M{"$regex": metric[0]}}).All(&alm)
			if len(alm) > 0 {
				go this.check_metric(alm)
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

func (this *Mongo) check_metric(alm []types.Alarm) {
	session := this.session.Copy()
	defer session.Close()
	for i := range alm {
		var value float64
		var stat int
		if alm[i].T == types.EXP {
			exps := cal.Parser(alm[i].Exp)
			k_v := make(map[string]float32)
			for i := range exps {
				if len(exps[i]) > 1 {
					result := this.get_values(exps[i], alm[i].P)
					if len(result) < 1 {
						return
					}
					k_v[exps[i]] = float32(types.Avg_value(result))
				}
			}
			t_value, _ := cal.Cal(alm[i].Exp, k_v)
			value = float64(t_value)
			stat = types.Judge_value(alm[i], value)
		} else {
			stat, value = this.check_value(alm[i])
		}
		go this.trigger(alm[i].Exp, stat, value)
	}
}

func (this *Mongo) check_value(v types.Alarm) (int, float64) {
	result := this.get_values(v.Exp, v.P)
	if len(result) < 1 {
		return 0, 0
	}
	var rst float64
	switch v.T {
	case types.AVG:
		{
			rst = types.Avg_value(result)
			return types.Judge_value(v, rst), rst
		}
	case types.SUM:
		{
			rst = types.Sum_value(result)
			return types.Judge_value(v, rst), rst
		}
	case types.MAX:
		{
			rst = types.Max_value(result)
			return types.Judge_value(v, rst), rst
		}
	case types.MIN:
		{
			min := types.Min_value(result)
			return types.Judge_value(v, rst), rst
		}
	}
	return 0, rst
}

func (this *Mongo) trigger(metric string, stat int, value float64) {
	var almaction types.AlarmAction
	session := this.session.Copy()
	defer session.Close()
	err := session.DB(this.dbname).C("AlarmAction").Find(bson.M{"exp": metric}).One(&almaction)
	if err == nil {
		if almaction.Stat != stat {
			go notify.Send(almaction, stat, value)
			_ = session.DB(this.dbname).C("AlarmAction").Update(bson.M{"exp": metric}, bson.M{"stat": stat})
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
