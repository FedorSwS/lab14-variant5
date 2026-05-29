#!/usr/bin/env python3
"""Kafka consumer for real-time log processing with sliding window"""

import json
import time
from collections import defaultdict, deque
from datetime import datetime

from kafka import KafkaConsumer


class SlidingWindow:
    def __init__(self, window_seconds: int = 300):
        self.window_seconds = window_seconds
        self.entries = deque()
        self.status_counts = defaultdict(int)
        self.path_counts = defaultdict(int)
        self.response_times = []

    def add(self, entry: dict):
        now = time.time()
        self.entries.append((now, entry))
        self.status_counts[entry.get("status", 0)] += 1
        path = self._extract_path(entry.get("request", ""))
        self.path_counts[path] += 1
        self.response_times.append(entry.get("response_time", 0))
        self._clean_old(now)

    def _clean_old(self, now: float):
        cutoff = now - self.window_seconds
        while self.entries and self.entries[0][0] < cutoff:
            _, old = self.entries.popleft()
            self.status_counts[old.get("status", 0)] -= 1
            old_path = self._extract_path(old.get("request", ""))
            self.path_counts[old_path] -= 1
            if self.path_counts[old_path] <= 0:
                del self.path_counts[old_path]

    def _extract_path(self, request: str) -> str:
        parts = request.split()
        return parts[1] if len(parts) > 1 else "/"

    def get_stats(self) -> dict:
        if not self.response_times:
            return {}
        recent_rt = self.response_times[-len(self.entries):]
        rt_sorted = sorted(recent_rt) if recent_rt else []
        top_path = max(self.path_counts.items(), key=lambda x: x[1]) if self.path_counts else ("/", 0)
        return {
            "total_requests": len(self.entries),
            "status_distribution": dict(self.status_counts),
            "top_path": top_path[0],
            "top_path_count": top_path[1],
            "avg_response_time": sum(recent_rt) / len(recent_rt) if recent_rt else 0,
            "p95_response_time": rt_sorted[int(len(rt_sorted) * 0.95)] if rt_sorted else 0,
        }


def main():
    consumer = KafkaConsumer(
        "logs",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="latest",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )
    window = SlidingWindow(window_seconds=300)
    print("Starting Kafka consumer (5-minute sliding window)...")
    for msg in consumer:
        entry = msg.value
        window.add(entry)
        stats = window.get_stats()
        if stats:
            print(f"[{datetime.now().isoformat()}] Requests: {stats['total_requests']}, "
                  f"Top: {stats['top_path']} ({stats['top_path_count']}), "
                  f"P95 RT: {stats['p95_response_time']:.3f}s")


if __name__ == "__main__":
    main()
