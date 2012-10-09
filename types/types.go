package types

const (
	AVG     = 1
	SUM     = 2
	MAX     = 3
	MIN     = 4
	AND     = 5
	OR      = 6
	XOR     = 7
	DIV     = 8
	LESS    = 9
	GREATER = 10
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
	M string  //Metric name
	T int     // AVG, SUM, MAX, MIN
	P int     //1min, 5min, 15min
	J int     //LESS, GREATER
	V float64 //value
}

type AlarmRelation struct {
	M1 string //metric1
	R  int    // AND, OR, XOR, DIV
	M2 string //metric2
}

type Address struct {
	T  string //email, phone, im
	ID string
}

type AlarmAction struct {
	M     string    //Metric name
	C     []Address //email,phone, im
	Count int
	Stat  int
	Ts    int64
}
