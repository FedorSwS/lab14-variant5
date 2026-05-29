package main

import (
	"sync"
	"time"
)

type WindowStats struct {
	Start           time.Time `json:"start"`
	End             time.Time `json:"end"`
	TotalRequests   int       `json:"total_requests"`
	AvgResponseTime float64   `json:"avg_response_time"`
	MaxResponseTime float64   `json:"max_response_time"`
	MinResponseTime float64   `json:"min_response_time"`
	Status2xx       int       `json:"status_2xx"`
	Status3xx       int       `json:"status_3xx"`
	Status4xx       int       `json:"status_4xx"`
	Status5xx       int       `json:"status_5xx"`
	UniqueIPs       int       `json:"unique_ips"`
	TopPath         string    `json:"top_path"`
}

type TumblingWindow struct {
	size    time.Duration
	entries chan LogEntry
	output  chan WindowStats
	current *windowBuf
	mu      sync.Mutex
	wg      sync.WaitGroup
	stopCh  chan struct{}
}

type windowBuf struct {
	start         time.Time
	entries       []LogEntry
	pathCounts    map[string]int
	ips           map[string]bool
	totalRespTime float64
	maxRespTime   float64
	minRespTime   float64
	statusCounts  map[int]int
}

func NewTumblingWindow(windowSize time.Duration) *TumblingWindow {
	tw := &TumblingWindow{
		size:    windowSize,
		entries: make(chan LogEntry, 10000),
		output:  make(chan WindowStats, 100),
		stopCh:  make(chan struct{}),
	}
	tw.reset()
	return tw
}

func (tw *TumblingWindow) reset() {
	now := time.Now()
	tw.current = &windowBuf{
		start:         now.Truncate(tw.size),
		entries:       make([]LogEntry, 0),
		pathCounts:    make(map[string]int),
		ips:           make(map[string]bool),
		statusCounts:  make(map[int]int),
		maxRespTime:   0,
		minRespTime:   1e9,
	}
}

func (tw *TumblingWindow) Start() {
	tw.wg.Add(2)
	go tw.processor()
	go tw.flusher()
}

func (tw *TumblingWindow) processor() {
	defer tw.wg.Done()
	for {
		select {
		case <-tw.stopCh:
			return
		case e := <-tw.entries:
			tw.mu.Lock()
			tw.add(e)
			tw.mu.Unlock()
		}
	}
}

func (tw *TumblingWindow) add(e LogEntry) {
	windowStart := e.Timestamp.Truncate(tw.size)
	if windowStart.After(tw.current.start) {
		tw.flush()
		tw.current.start = windowStart
	}
	tw.current.entries = append(tw.current.entries, e)
	tw.current.totalRespTime += e.ResponseTime
	if e.ResponseTime > tw.current.maxRespTime {
		tw.current.maxRespTime = e.ResponseTime
	}
	if e.ResponseTime < tw.current.minRespTime {
		tw.current.minRespTime = e.ResponseTime
	}
	path := extractPath(e.Request)
	tw.current.pathCounts[path]++
	tw.current.ips[e.RemoteAddr] = true
	tw.current.statusCounts[e.Status]++
}

func (tw *TumblingWindow) flusher() {
	defer tw.wg.Done()
	ticker := time.NewTicker(tw.size)
	defer ticker.Stop()
	for {
		select {
		case <-tw.stopCh:
			tw.mu.Lock()
			tw.flush()
			tw.mu.Unlock()
			return
		case <-ticker.C:
			tw.mu.Lock()
			tw.flush()
			tw.mu.Unlock()
		}
	}
}

func (tw *TumblingWindow) flush() {
	if len(tw.current.entries) == 0 {
		return
	}
	topPath, topCount := "", 0
	for p, c := range tw.current.pathCounts {
		if c > topCount {
			topCount = c
			topPath = p
		}
	}
	stats := WindowStats{
		Start:           tw.current.start,
		End:             tw.current.start.Add(tw.size),
		TotalRequests:   len(tw.current.entries),
		AvgResponseTime: tw.current.totalRespTime / float64(len(tw.current.entries)),
		MaxResponseTime: tw.current.maxRespTime,
		MinResponseTime: tw.current.minRespTime,
		Status2xx:       tw.current.statusCounts[200] + tw.current.statusCounts[201] + tw.current.statusCounts[204],
		Status3xx:       tw.current.statusCounts[301] + tw.current.statusCounts[302] + tw.current.statusCounts[304],
		Status4xx:       tw.current.statusCounts[400] + tw.current.statusCounts[403] + tw.current.statusCounts[404],
		Status5xx:       tw.current.statusCounts[500] + tw.current.statusCounts[502] + tw.current.statusCounts[503],
		UniqueIPs:       len(tw.current.ips),
		TopPath:         topPath,
	}
	select {
	case tw.output <- stats:
	default:
	}
	tw.current.entries = tw.current.entries[:0]
	for k := range tw.current.pathCounts {
		delete(tw.current.pathCounts, k)
	}
	for k := range tw.current.ips {
		delete(tw.current.ips, k)
	}
	for k := range tw.current.statusCounts {
		delete(tw.current.statusCounts, k)
	}
	tw.current.totalRespTime = 0
	tw.current.maxRespTime = 0
	tw.current.minRespTime = 1e9
}

func (tw *TumblingWindow) Submit(e LogEntry) {
	select {
	case tw.entries <- e:
	default:
	}
}

func (tw *TumblingWindow) Output() <-chan WindowStats {
	return tw.output
}

func (tw *TumblingWindow) Stop() {
	close(tw.stopCh)
	tw.wg.Wait()
	close(tw.entries)
	close(tw.output)
}

func extractPath(request string) string {
	for i, c := range request {
		if c == ' ' {
			for j := i + 1; j < len(request); j++ {
				if request[j] == ' ' {
					return request[i+1 : j]
				}
			}
			break
		}
	}
	return "/unknown"
}
