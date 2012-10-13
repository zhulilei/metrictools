package types

const (
	AVG     = 1
	SUM     = 2
	MAX     = 3
	MIN     = 4
	EXP     = 5
	LESS    = 6
	GREATER = 7
)

type Message struct {
	Done    chan int
	Content string
}

type Record struct {
	Rt string
	Nm string
	Cl string
	Hs string
	V  float64
	Ts int64
}

type Metric struct {
	Record
	App string
}

type Host struct {
	Host   string
	Metric string
	Ttl    int
}

type Alarm struct {
	Exp string    //Metric expression name
	T   int       //AVG, SUM, MAX, MIN
	P   int       //1min, 5min, 15min
	J   int       //LESS, GREATER
	V   []float64 //value
}

type Action struct {
	T  string //email, phone, im, mq
	Nm string // email address, phone number, im id, mq name
}

type AlarmAction struct {
	Exp  string //Metric expression name
	Type string //nginx_cpu, nginx_req, apache_cpu, apache_req, lvs_netio, app_cpu, etc.
	Pd   string //blog, photo, reader
	Act  [][]byte //action json
	Stat int
	Ts   int64
}
