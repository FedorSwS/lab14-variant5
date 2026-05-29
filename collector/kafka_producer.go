package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/segmentio/kafka-go"
)

// KafkaProducer отправляет агрегированные статистики в Kafka топик
type KafkaProducer struct {
	writer  *kafka.Writer
	topic   string
	enabled bool
}

// NewKafkaProducer создаёт новый Kafka продюсер
func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	// Если нет брокеров - продюсер отключён
	if len(brokers) == 0 || brokers[0] == "" {
		log.Println("Kafka producer disabled: no brokers configured")
		return &KafkaProducer{enabled: false}
	}

	writer := &kafka.Writer{
		Addr:         kafka.TCP(brokers...),
		Topic:        topic,
		Balancer:     &kafka.LeastBytes{},
		BatchSize:    100,
		BatchTimeout: 500 * time.Millisecond,
		RequiredAcks: kafka.RequireOne,
	}

	log.Printf("Kafka producer initialized: brokers=%v, topic=%s", brokers, topic)
	return &KafkaProducer{
		writer:  writer,
		topic:   topic,
		enabled: true,
	}
}

// SendWindowStats отправляет агрегированные статистики в Kafka
func (kp *KafkaProducer) SendWindowStats(ctx context.Context, stats WindowStats) error {
	if !kp.enabled {
		return nil
	}

	data, err := json.Marshal(stats)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(stats.TopPath),
		Value: data,
		Time:  time.Now(),
	}

	return kp.writer.WriteMessages(ctx, msg)
}

// SendLogEntry отправляет сырой лог в Kafka (опционально)
func (kp *KafkaProducer) SendLogEntry(ctx context.Context, entry LogEntry) error {
	if !kp.enabled {
		return nil
	}

	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(entry.RemoteAddr),
		Value: data,
		Time:  time.Now(),
	}

	return kp.writer.WriteMessages(ctx, msg)
}

// Close закрывает Kafka продюсер
func (kp *KafkaProducer) Close() error {
	if kp.enabled && kp.writer != nil {
		return kp.writer.Close()
	}
	return nil
}

// GetKafkaBrokersFromEnv читает брокеры из переменной окружения
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
