package main

import (
	"context"
	"encoding/json"
	"log"
	"sync"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"go.etcd.io/etcd/client/v3/concurrency"
)

type Shard struct {
	ID       string   `json:"id"`
	Paths    []string `json:"paths"`
	WorkerID string   `json:"worker_id"`
}

type DistributedCollector struct {
	client    *clientv3.Client
	session   *concurrency.Session
	workerID  string
	shard     *Shard
	mu        sync.RWMutex
	collector *Collector
	stopCh    chan struct{}
}

func NewDistributedCollector(endpoints []string, workerID string, c *Collector) (*DistributedCollector, error) {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   endpoints,
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		return nil, err
	}
	session, err := concurrency.NewSession(cli, concurrency.WithTTL(10))
	if err != nil {
		return nil, err
	}
	return &DistributedCollector{
		client:    cli,
		session:   session,
		workerID:  workerID,
		collector: c,
		stopCh:    make(chan struct{}),
	}, nil
}

func (dc *DistributedCollector) Register(ctx context.Context) error {
	lease := clientv3.NewLease(dc.client)
	resp, err := lease.Grant(ctx, 10)
	if err != nil {
		return err
	}
	_, err = dc.client.Put(ctx, "/workers/"+dc.workerID, dc.workerID, clientv3.WithLease(resp.ID))
	if err != nil {
		return err
	}
	go dc.keepAlive(ctx, lease, resp.ID)
	return nil
}

func (dc *DistributedCollector) keepAlive(ctx context.Context, lease clientv3.Lease, id clientv3.LeaseID) {
	ch, _ := lease.KeepAlive(ctx, id)
	for range ch {
	}
}

func (dc *DistributedCollector) AcquireShard(ctx context.Context) error {
	mutex := concurrency.NewMutex(dc.session, "/locks/master")
	if err := mutex.Lock(ctx); err != nil {
		return err
	}
	defer mutex.Unlock(ctx)

	resp, _ := dc.client.Get(ctx, "/shards/", clientv3.WithPrefix())
	assigned := make(map[string]bool)
	for _, kv := range resp.Kvs {
		var s Shard
		json.Unmarshal(kv.Value, &s)
		assigned[s.ID] = true
	}

	for i := 0; i < 10; i++ {
		id := string(rune('A' + i))
		if !assigned[id] {
			shard := Shard{ID: id, Paths: shardPaths(id), WorkerID: dc.workerID}
			data, _ := json.Marshal(shard)
			dc.client.Put(ctx, "/shards/"+id, string(data), clientv3.WithLease(dc.session.Lease()))
			dc.mu.Lock()
			dc.shard = &shard
			dc.mu.Unlock()
			log.Printf("Worker %s acquired shard %s", dc.workerID, id)
			return nil
		}
	}
	return nil
}

func (dc *DistributedCollector) Process(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-dc.stopCh:
			return
		case <-ticker.C:
			dc.mu.RLock()
			s := dc.shard
			dc.mu.RUnlock()
			if s != nil {
				for _, path := range s.Paths {
					dc.collector.Submit(LogEntry{
						RemoteAddr:   "10.0.0.1",
						Request:      "GET " + path + " HTTP/1.1",
						Status:       200,
						ResponseTime: 0.05,
						Timestamp:    time.Now(),
					})
				}
			}
		}
	}
}

func (dc *DistributedCollector) Release(ctx context.Context) error {
	if dc.shard != nil {
		_, err := dc.client.Delete(ctx, "/shards/"+dc.shard.ID)
		return err
	}
	return nil
}

func (dc *DistributedCollector) Close() {
	close(dc.stopCh)
	dc.client.Close()
}

func shardPaths(id string) []string {
	m := map[string][]string{
		"A": {"/", "/index.html"},
		"B": {"/api/users", "/api/user/profile"},
		"C": {"/api/products", "/api/product/details"},
		"D": {"/static/css", "/static/js"},
		"E": {"/admin", "/admin/dashboard"},
		"F": {"/login", "/logout", "/auth"},
		"G": {"/blog", "/blog/post"},
		"H": {"/download", "/files"},
		"I": {"/api/v2/users", "/api/v2/orders"},
		"J": {"/health", "/metrics", "/debug"},
	}
	if p, ok := m[id]; ok {
		return p
	}
	return []string{"/other/" + id}
}
