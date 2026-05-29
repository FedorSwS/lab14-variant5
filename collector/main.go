package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	output := flag.String("output", "data/raw/logs.jsonl", "output file")
	bufSize := flag.Int("buffer", 1000, "buffer size")
	flushInt := flag.Duration("flush", 5*time.Second, "flush interval")
	workers := flag.Int("workers", 4, "worker count")
	rate := flag.Int("rate", 100, "logs per second")
	windowSize := flag.Duration("window", 5*time.Second, "tumbling window size")
	kafkaTopic := flag.String("kafka-topic", "logs", "Kafka topic name")
	flag.Parse()

	log.Printf("Starting collector: workers=%d, rate=%d, window=%v", *workers, *rate, *windowSize)

	collector, err := NewCollector(*output, *bufSize, *flushInt)
	if err != nil {
		log.Fatal(err)
	}
	collector.Start(*workers)

	window := NewTumblingWindow(*windowSize)
	window.Start()

	kafkaProducer := NewKafkaProducer(GetKafkaBrokersFromEnv(), *kafkaTopic)

	go func() {
		for stats := range window.Output() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := kafkaProducer.SendWindowStats(ctx, stats); err != nil {
				log.Printf("Kafka send error: %v", err)
			}
			cancel()
			log.Printf("[Window] Requests: %d, AvgRT: %.3fs, TopPath: %s",
				stats.TotalRequests, stats.AvgResponseTime, stats.TopPath)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ticker := time.NewTicker(time.Second / time.Duration(*rate))
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
					RemoteAddr:   generateIP(count),
					Request:      generateRequest(paths, count),
					Status:       statuses[count%len(statuses)],
					ResponseTime: float64(10+count%490) / 1000.0,
					UserAgent:    "Mozilla/5.0",
					Timestamp:    time.Now(),
				}
				collector.Submit(entry)
				window.Submit(entry)

				if kafkaProducer.Enabled {
					go func(e LogEntry) {
						ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
						kafkaProducer.SendLogEntry(ctx, e)
						cancel()
					}(entry)
				}
				count++
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("Shutting down gracefully...")
	cancel()
	window.Stop()
	collector.Close()
	kafkaProducer.Close()
	log.Println("Done")
}

func generateIP(count int) string {
	return "192.168.1." + string(rune('0'+count%255))
}

func generateRequest(paths []string, count int) string {
	return "GET " + paths[count%len(paths)] + " HTTP/1.1"
}
