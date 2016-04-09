package main

import (
	"net"
	"flag"
	"os"
	"syscall"
	"errors"
	"github.com/datastream/skyline"
	"github.com/influxdata/kapacitor/udf"
	"github.com/influxdata/kapacitor/udf/agent"
	"log"
)

// Find skyline via the skyline alg
type skylineHandler struct {
	field        string
	consensus    int64
	fullDuration int64
	minDuration  int64
	state        *SkylineState
	agent        *agent.Agent
}

type SkylineState struct {
	Points []*udf.Point
}

func (s SkylineState) reset() {
	s.Points = []*udf.Point{}
}

func newSkylineHandler(agent *agent.Agent) *skylineHandler {
	return &skylineHandler{agent: agent, state: &SkylineState{}, consensus: 6, fullDuration: 86400, minDuration: 10800}
}

// Return the InfoResponse. Describing the properties of this UDF agent.
func (*skylineHandler) Info() (*udf.InfoResponse, error) {
	info := &udf.InfoResponse{
		Wants:    udf.EdgeType_BATCH,
		Provides: udf.EdgeType_BATCH,
		Options: map[string]*udf.OptionInfo{
			"field":         {ValueTypes: []udf.ValueType{udf.ValueType_STRING}},
			"full_duration": {ValueTypes: []udf.ValueType{udf.ValueType_INT}},
			"min_duration":  {ValueTypes: []udf.ValueType{udf.ValueType_INT}},
			"consensus":     {ValueTypes: []udf.ValueType{udf.ValueType_INT}},
		},
	}
	return info, nil
}

// Initialze the handler based of the provided options.
func (o *skylineHandler) Init(r *udf.InitRequest) (*udf.InitResponse, error) {
	init := &udf.InitResponse{
		Success: true,
		Error:   "",
	}
	for _, opt := range r.Options {
		switch opt.Name {
		case "field":
			o.field = opt.Values[0].Value.(*udf.OptionValue_StringValue).StringValue
		case "full_duration":
			o.fullDuration = opt.Values[0].Value.(*udf.OptionValue_IntValue).IntValue
		case "min_duration":
			o.minDuration = opt.Values[0].Value.(*udf.OptionValue_IntValue).IntValue
		case "consensus":
			o.consensus = opt.Values[0].Value.(*udf.OptionValue_IntValue).IntValue
		}
	}

	if o.field == "" {
		init.Success = false
		init.Error = "must supply field"
	}
	if o.consensus < 1 {
		init.Success = false
		init.Error += " invalid consensus, must be >= 1"
	}

	return init, nil
}

// Create a snapshot of the running state of the process.
func (o *skylineHandler) Snaphost() (*udf.SnapshotResponse, error) {
	return &udf.SnapshotResponse{}, nil
}

// Restore a previous snapshot.
func (o *skylineHandler) Restore(req *udf.RestoreRequest) (*udf.RestoreResponse, error) {
	return &udf.RestoreResponse{
		Success: true,
	}, nil
}

// Start working with the next batch
func (o *skylineHandler) BeginBatch(begin *udf.BeginBatch) error {
	o.state.reset()
	// Send BeginBatch response to Kapacitor
	// We always send a batch back for every batch we receive
	o.agent.Responses <- &udf.Response{
		Message: &udf.Response_Begin{
			Begin: begin,
		},
	}

	return nil
}

type accpeter struct {
	count int64
}

func (o *skylineHandler) Point(p *udf.Point) error {
	o.state.Points = append(o.state.Points, p)
	return nil
}
func (o *skylineHandler) EndBatch(end *udf.EndBatch) error {
	rst, err := SkylineCheck(o.state.Points, o.field, o.fullDuration, o.consensus)
	if err != nil {
		return err
	}
	pt := o.state.Points[len(o.state.Points)-1]
	p := &udf.Point{
		Name:         pt.Name,
		Time:         pt.Time,
		Group:        pt.Group,
		Tags:         pt.Tags,
		FieldsDouble: pt.FieldsDouble,
	}
	p.FieldsDouble["isAnomaly"] = 0
	if err == nil && rst {
		p.FieldsDouble["isAnomaly"] = 1
	}
	o.agent.Responses <- &udf.Response{
		Message: &udf.Response_Point{
			Point: p,
		},
	}

	// End batch
	o.agent.Responses <- &udf.Response{
		Message: &udf.Response_End{
			End: end,
		},
	}
	return nil
}

type timePoint struct {
	t int64
	v float64
}

func (t timePoint) GetValue() float64 {
	return t.v
}
func (t timePoint) GetTimestamp() int64 {
	return t.t
}

func SkylineCheck(points []*udf.Point, field string, fullDuration int64, consensus int64) (bool, error) {
	var timeseries []skyline.TimePoint
	var timepoint timePoint
	for _, point := range points {
		timepoint.t = point.Time
		timepoint.v = point.FieldsDouble[field]
		timeseries = append(timeseries, timepoint)
	}
	if len(timeseries) == 0 {
		log.Println("null data")
		return false, errors.New("null data")
	}
	var rst []int
	if skyline.MedianAbsoluteDeviation(timeseries) {
		rst = append(rst, 1)
	} else {
		rst = append(rst, 0)
	}
	if skyline.Grubbs(timeseries) {
		rst = append(rst, 1)
	} else {
		rst = append(rst, 0)
	}
	if skyline.FirstHourAverage(timeseries, fullDuration) {
		rst = append(rst, 1)
	} else {
		rst = append(rst, 0)
	}
	if skyline.SimpleStddevFromMovingAverage(timeseries) {
		rst = append(rst, 1)
	} else {
		rst = append(rst, 0)
	}
	if skyline.StddevFromMovingAverage(timeseries) {
		rst = append(rst, 1)
	} else {
		rst = append(rst, 0)
	}
	if skyline.MeanSubtractionCumulation(timeseries) {
		rst = append(rst, 1)
	} else {
		rst = append(rst, 0)
	}
	if skyline.LeastSquares(timeseries) {
		rst = append(rst, 1)
	} else {
		rst = append(rst, 0)
	}
	if skyline.HistogramBins(timeseries) {
		rst = append(rst, 1)
	} else {
		rst = append(rst, 0)
	}
	return CheckThreshhold(rst, 8-int(consensus)), nil
}

func CheckThreshhold(data []int, threshold int) bool {
	stat := 0
	for _, v := range data {
		if v == 1 {
			stat++
		}
	}
	if (len(data) - stat) <= threshold {
		return true
	}
	return false
}

// Stop the handler gracefully.
func (o *skylineHandler) Stop() {
	close(o.agent.Responses)
}
// Create a new agent/handler for each new connection.
// Count and log each new connection and termination.
func (acc *accpeter) Accept(conn net.Conn) {
	count := acc.count
	acc.count++
	a := agent.New(conn, conn)
	h := newSkylineHandler(a)
	a.Handler = h

	log.Println("Starting agent for connection", count)
	a.Start()
	go func() {
		err := a.Wait()
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("Agent for connection %d finished", count)
	}()
}

var socketPath = flag.String("socket", "/tmp/skyline.sock", "Where to create the unix socket")

func main() {
	flag.Parse()

	// Create unix socket
	addr, err := net.ResolveUnixAddr("unix", *socketPath)
	if err != nil {
		log.Fatal(err)
	}
	l, err := net.ListenUnix("unix", addr)
	if err != nil {
		log.Fatal(err)
	}

	// Create server that listens on the socket
	s := agent.NewServer(l, &accpeter{})

	// Setup signal handler to stop Server on various signals
	s.StopOnSignals(os.Interrupt, syscall.SIGTERM)

	log.Println("Server listening on", addr.String())
	err = s.Serve()
	if err != nil {
		log.Fatal(err)
	}
	log.Println("Server stopped")
}
