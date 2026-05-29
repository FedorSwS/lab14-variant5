package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	writer  *kafka.Writer
	topic   string
	Enabled bool
}

func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	if len(brokers) == 0 || brokers[0] == "" {
		log.Println("Kafka producer disabled: KAFKA_BOOTSTRAP not set")
		return &KafkaProducer{Enabled: false}
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 500 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}

	log.Printf("Kafka producer enabled: brokers=%v, topic=%s", brokers, topic)
	return &KafkaProducer{
		writer:  writer,
		topic:   topic,
		Enabled: true,
	}
}

func (kp *KafkaProducer) SendWindowStats(ctx context.Context, stats WindowStats) error {
	if !kp.Enabled {
		return nil
	}
	data, err := json.Marshal(stats)
	if err != nil {
		return err
	}
	return kp.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(stats.TopPath),
		Value: data,
		Time:  time.Now(),
	})
}

func (kp *KafkaProducer) SendLogEntry(ctx context.Context, entry LogEntry) error {
	if !kp.Enabled {
		return nil
	}
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}
	return kp.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(entry.RemoteAddr),
		Value: data,
		Time:  time.Now(),
	})
}

func (kp *KafkaProducer) Close() error {
	if kp.Enabled && kp.writer != nil {
		return kp.writer.Close()
	}
	return nil
}

func GetKafkaBrokersFromEnv() []string {
	brokers := os.Getenv("KAFKA_BOOTSTRAP")
	if brokers == "" {
		brokers = os.Getenv("KAFKA_BROKERS")
	}
	if brokers == "" {
		return nil
	}
	return []string{brokers}
}
