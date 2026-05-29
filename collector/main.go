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

	// Создаём коллектор
	collector, err := NewCollector(*output, *bufSize, *flushInt)
	if err != nil {
		log.Fatal(err)
	}
	collector.Start(*workers)

	// Создаём оконный агрегатор
	window := NewTumblingWindow(*windowSize)
	window.Start()

	// Создаём Kafka продюсер
	kafkaProducer := NewKafkaProducer(GetKafkaBrokersFromEnv(), *kafkaTopic)

	// Запускаем горутину для отправки агрегированных данных в Kafka
	go func() {
		for stats := range window.Output() {
			// Отправляем в Kafka
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := kafkaProducer.SendWindowStats(ctx, stats); err != nil {
				log.Printf("Failed to send to Kafka: %v", err)
			}
			cancel()

			// Логируем для отладки
			log.Printf("[Window] Requests: %d, AvgRT: %.3fs, TopPath: %s, Status2xx: %d",
				stats.TotalRequests, stats.AvgResponseTime, stats.TopPath, stats.Status2xx)
		}
	}()

	// Генератор логов (сырые данные идут и в окна, и в Kafka)
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
				// Отправляем в коллектор (для JSONL файла)
				collector.Submit(entry)
				// Отправляем в оконный агрегатор
				window.Submit(entry)
				// Отправляем в Kafka (сырые логи)
				go func(e LogEntry) {
					ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
					kafkaProducer.SendLogEntry(ctx, e)
					cancel()
				}(entry)
				count++
			}
		}
	}()

	// Graceful shutdown
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
