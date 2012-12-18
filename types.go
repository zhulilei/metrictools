package metrictools

const (
	AVG     = 1
	SUM     = 2
	MAX     = 3
	MIN     = 4
	EXP     = 5
	LESS    = 6
	GREATER = 7
)

type Record struct {
	Nm string
	Cl string
	Hs string
	V  float64
	Ts int64
}

type Metric struct {
	Record
	Retention string
	App       string
}

type Host struct {
	Host   string
	Metric string
	Ttl    int
}

type Trigger struct {
	Exp  string    //metric expressions
	T    int       //AVG, SUM, MAX, MIN
	P    int       //1min, 5min, 15min
	J    int       //LESS, GREATER
	I    int       //check interval time: 1min, 5min, 15min
	V    []float64 //value
	Nm   string    //nginx_cpu, nginx_req, apache_cpu, apache_req, lvs_netio, app_cpu, etc.
	Pd   string    //blog, photo, reader, etc.
	Last int64     //last modify time
	Stat int       //last trigger stat
}
type AlarmAction struct {
	Exp string
	T   string //email, phone, im, mq
	Nm  string //email address, phone number, im id, mq name
	Ts  int64  //last send
}
