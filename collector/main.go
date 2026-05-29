package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

type LogEntry struct {
	RemoteAddr   string    `json:"remote_addr"`
	Request      string    `json:"request"`
	Status       int       `json:"status"`
	ResponseTime float64   `json:"response_time"`
	UserAgent    string    `json:"user_agent"`
	Timestamp    time.Time `json:"timestamp"`
}

type BatchWriter struct {
	file          *os.File
	writer        *bufio.Writer
	buffer        []LogEntry
	bufferSize    int
	flushInterval time.Duration
	mu            sync.Mutex
	stopCh        chan struct{}
	wg            sync.WaitGroup
}

func NewBatchWriter(path string, size int, interval time.Duration) (*BatchWriter, error) {
	os.MkdirAll("data/raw", 0755)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	bw := &BatchWriter{
		file:          f,
		writer:        bufio.NewWriterSize(f, 65536),
		buffer:        make([]LogEntry, 0, size),
		bufferSize:    size,
		flushInterval: interval,
		stopCh:        make(chan struct{}),
	}
	bw.wg.Add(1)
	go bw.flushLoop()
	return bw, nil
}

func (bw *BatchWriter) flushLoop() {
	defer bw.wg.Done()
	ticker := time.NewTicker(bw.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			bw.Flush()
		case <-bw.stopCh:
			bw.Flush()
			return
		}
	}
}

func (bw *BatchWriter) Add(entry LogEntry) {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	bw.buffer = append(bw.buffer, entry)
	if len(bw.buffer) >= bw.bufferSize {
		bw.flushLocked()
	}
}

func (bw *BatchWriter) Flush() {
	bw.mu.Lock()
	defer bw.mu.Unlock()
	bw.flushLocked()
}

func (bw *BatchWriter) flushLocked() {
	if len(bw.buffer) == 0 {
		return
	}
	for _, e := range bw.buffer {
		data, _ := json.Marshal(e)
		bw.writer.Write(append(data, '\n'))
	}
	bw.writer.Flush()
	bw.buffer = bw.buffer[:0]
}

func (bw *BatchWriter) Close() {
	close(bw.stopCh)
	bw.wg.Wait()
	bw.Flush()
	bw.file.Close()
}

type Collector struct {
	entries chan LogEntry
	writer  *BatchWriter
	wg      sync.WaitGroup
	ctx     context.Context
	cancel  context.CancelFunc
}

func NewCollector(output string, bufSize int, flushInterval time.Duration) (*Collector, error) {
	bw, err := NewBatchWriter(output, bufSize, flushInterval)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Collector{
		entries: make(chan LogEntry, bufSize*2),
		writer:  bw,
		ctx:     ctx,
		cancel:  cancel,
	}, nil
}

func (c *Collector) Start(workers int) {
	for i := 0; i < workers; i++ {
		c.wg.Add(1)
		go c.worker()
	}
}

func (c *Collector) worker() {
	defer c.wg.Done()
	for {
		select {
		case entry, ok := <-c.entries:
			if !ok {
				return
			}
			c.writer.Add(entry)
		case <-c.ctx.Done():
			return
		}
	}
}

func (c *Collector) Submit(entry LogEntry) {
	select {
	case c.entries <- entry:
	case <-c.ctx.Done():
	}
}

func (c *Collector) Close() {
	c.cancel()
	close(c.entries)
	c.wg.Wait()
	c.writer.Close()
}

func generateLogs(ctx context.Context, c *Collector, rate int) {
	ticker := time.NewTicker(time.Second / time.Duration(rate))
	defer ticker.Stop()

	paths := []string{"/", "/api/users", "/api/products", "/admin", "/login"}
	statuses := []int{200, 200, 200, 301, 400, 404, 500}
	count := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			entry := LogEntry{
				RemoteAddr:   fmt.Sprintf("192.168.1.%d", count%255),
				Request:      fmt.Sprintf("GET %s HTTP/1.1", paths[count%len(paths)]),
				Status:       statuses[count%len(statuses)],
				ResponseTime: float64(10+count%490) / 1000.0,
				UserAgent:    "Mozilla/5.0",
				Timestamp:    time.Now(),
			}
			c.Submit(entry)
			count++
		}
	}
}

func main() {
	output := flag.String("output", "data/raw/logs.jsonl", "output file")
	bufSize := flag.Int("buffer", 1000, "buffer size")
	flushInt := flag.Duration("flush", 5*time.Second, "flush interval")
	workers := flag.Int("workers", 4, "worker count")
	rate := flag.Int("rate", 100, "logs per second")
	flag.Parse()

	log.Printf("Starting collector: workers=%d, rate=%d", *workers, *rate)

	collector, err := NewCollector(*output, *bufSize, *flushInt)
	if err != nil {
		log.Fatal(err)
	}
	collector.Start(*workers)

	ctx, cancel := context.WithCancel(context.Background())
	go generateLogs(ctx, collector, *rate)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down gracefully...")
	cancel()
	collector.Close()
	log.Println("Done")
}
