package main

import (
	"fmt"
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
	points []*udf.Point
	result bool
}

func (s SkylineState) reset() {
	s.points = []*udf.Point{}
	s.result = true
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

func (o *skylineHandler) Point(p *udf.Point) error {
	o.state.points = append(o.state.points, p)
	return nil
}
func (o *skylineHandler) EndBatch(end *udf.EndBatch) error {
	rst, err := SkylineCheck(o.state.points, o.field, o.fullDuration, o.consensus)
	if err != nil {
		log.Println(err)
	}
	pt := o.state.points[len(o.state.points)-1]
	p := &udf.Point{
		Name:         pt.Name,
		Time:         pt.Time,
		Group:        pt.Group,
		Tags:         pt.Tags,
		FieldsDouble: pt.FieldsDouble,
	}
	if err == nil && rst {
		p.Tags["isAnomaly"] = "1"
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
		return false, fmt.Errorf("null data")
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

func main() {
	a := agent.New()
	h := newSkylineHandler(a)
	a.Handler = h

	log.Println("Starting agent")
	a.Start()
	err := a.Wait()
	if err != nil {
		log.Fatal(err)
	}
}
