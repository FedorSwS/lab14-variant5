package main

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBatchWriter(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "test.jsonl")

	bw, _ := NewBatchWriter(path, 10, 100*time.Millisecond)
	for i := 0; i < 25; i++ {
		bw.Add(LogEntry{Status: 200})
	}
	time.Sleep(200 * time.Millisecond)
	bw.Close()

	data, _ := os.ReadFile(path)
	count := 0
	for _, b := range data {
		if b == '\n' {
			count++
		}
	}
	if count != 25 {
		t.Errorf("Expected 25 lines, got %d", count)
	}
}

func TestCollectorSubmit(t *testing.T) {
	tmp := t.TempDir()
	path := filepath.Join(tmp, "test.jsonl")

	c, _ := NewCollector(path, 50, 200*time.Millisecond)
	c.Start(2)

	for i := 0; i < 100; i++ {
		c.Submit(LogEntry{Status: 200})
	}
	time.Sleep(300 * time.Millisecond)
	c.Close()

	data, _ := os.ReadFile(path)
	count := 0
	for _, b := range data {
		if b == '\n' {
			count++
		}
	}
	if count != 100 {
		t.Errorf("Expected 100, got %d", count)
	}
}
