package main

import (
	"context"
	"net"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

type FlightServer struct {
	flight.BaseFlightServer
	window *TumblingWindow
	pool   memory.Allocator
}

func NewFlightServer(w *TumblingWindow) *FlightServer {
	return &FlightServer{
		window: w,
		pool:   memory.NewGoAllocator(),
	}
}

func (s *FlightServer) GetFlightInfo(ctx context.Context, req *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	schema := s.getSchema()
	serialized, err := flight.SerializeSchema(schema, s.pool)
	if err != nil {
		return nil, err
	}
	return &flight.FlightInfo{
		Schema: serialized,
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.FlightDescriptor_PATH,
			Path: req.Path,
		},
		Endpoint: []*flight.FlightEndpoint{{
			Ticket: &flight.Ticket{Ticket: []byte("stats")},
		}},
	}, nil
}

func (s *FlightServer) DoGet(ctx context.Context, ticket *flight.Ticket) (*arrow.RecordReader, error) {
	stats := make([]WindowStats, 0)
	for {
		select {
		case stat, ok := <-s.window.Output():
			if !ok {
				goto build
			}
			stats = append(stats, stat)
		default:
			goto build
		}
	}
build:
	record := s.buildRecord(stats)
	defer record.Release()
	return array.NewRecordReader(s.getSchema(), []arrow.Record{record})
}

func (s *FlightServer) getSchema() *arrow.Schema {
	return arrow.NewSchema(
		[]arrow.Field{
			{Name: "window_start", Type: arrow.FixedWidthTypes.Timestamp_us},
			{Name: "window_end", Type: arrow.FixedWidthTypes.Timestamp_us},
			{Name: "total_requests", Type: arrow.PrimitiveTypes.Int64},
			{Name: "avg_response_time", Type: arrow.PrimitiveTypes.Float64},
			{Name: "max_response_time", Type: arrow.PrimitiveTypes.Float64},
			{Name: "min_response_time", Type: arrow.PrimitiveTypes.Float64},
			{Name: "status_2xx", Type: arrow.PrimitiveTypes.Int64},
			{Name: "status_3xx", Type: arrow.PrimitiveTypes.Int64},
			{Name: "status_4xx", Type: arrow.PrimitiveTypes.Int64},
			{Name: "status_5xx", Type: arrow.PrimitiveTypes.Int64},
			{Name: "unique_ips", Type: arrow.PrimitiveTypes.Int64},
			{Name: "top_path", Type: arrow.BinaryTypes.String},
		},
		nil,
	)
}

func (s *FlightServer) buildRecord(stats []WindowStats) arrow.Record {
	builder := array.NewRecordBuilder(s.pool, s.getSchema())
	defer builder.Release()

	startB := builder.Field(0).(*array.TimestampBuilder)
	endB := builder.Field(1).(*array.TimestampBuilder)
	reqB := builder.Field(2).(*array.Int64Builder)
	avgB := builder.Field(3).(*array.Float64Builder)
	maxB := builder.Field(4).(*array.Float64Builder)
	minB := builder.Field(5).(*array.Float64Builder)
	s2xxB := builder.Field(6).(*array.Int64Builder)
	s3xxB := builder.Field(7).(*array.Int64Builder)
	s4xxB := builder.Field(8).(*array.Int64Builder)
	s5xxB := builder.Field(9).(*array.Int64Builder)
	ipB := builder.Field(10).(*array.Int64Builder)
	pathB := builder.Field(11).(*array.StringBuilder)

	for _, s := range stats {
		startB.Append(arrow.Timestamp(s.Start.UnixMicro()))
		endB.Append(arrow.Timestamp(s.End.UnixMicro()))
		reqB.Append(int64(s.TotalRequests))
		avgB.Append(s.AvgResponseTime)
		maxB.Append(s.MaxResponseTime)
		minB.Append(s.MinResponseTime)
		s2xxB.Append(int64(s.Status2xx))
		s3xxB.Append(int64(s.Status3xx))
		s4xxB.Append(int64(s.Status4xx))
		s5xxB.Append(int64(s.Status5xx))
		ipB.Append(int64(s.UniqueIPs))
		pathB.Append(s.TopPath)
	}
	return builder.NewRecord()
}

func RunFlightServer(window *TumblingWindow, port string) error {
	s := NewFlightServer(window)
	server := flight.NewFlightServer(nil)
	server.Init(s)
	lis, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	return server.Serve(lis)
}
