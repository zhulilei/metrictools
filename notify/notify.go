package notify

type Notify struct {
	Exp    string //metric expressions
	Level  int
	Value  float64
}

func (this *Notify) Send(msg_chan chan []byte, repeated bool) {
}
