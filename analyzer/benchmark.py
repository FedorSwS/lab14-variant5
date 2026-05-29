#!/usr/bin/env python3
"""Performance benchmark: Go vs Python Collector"""

import subprocess
import time
import psutil
import threading
import tempfile
from pathlib import Path


def benchmark_go_collector(duration: int = 10, rate: int = 500):
    with tempfile.NamedTemporaryFile(suffix='.jsonl', delete=False) as f:
        output = f.name

    process = subprocess.Popen(
        ["./collector/collector", "-output", output, "-rate", str(rate), "-workers", "8"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    cpu_samples, mem_samples = [], []

    def monitor():
        for _ in range(duration):
            try:
                cpu_samples.append(psutil.Process(process.pid).cpu_percent(interval=0.5))
                mem_samples.append(psutil.Process(process.pid).memory_info().rss / 1024 / 1024)
            except:
                pass
            time.sleep(0.5)

    monitor_thread = threading.Thread(target=monitor)
    monitor_thread.start()

    time.sleep(duration)
    process.terminate()
    process.wait(timeout=5)
    monitor_thread.join()

    with open(output, 'r') as f:
        lines = f.readlines()

    Path(output).unlink()

    return {
        "language": "Go",
        "total_entries": len(lines),
        "avg_cpu_percent": sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0,
        "max_cpu_percent": max(cpu_samples) if cpu_samples else 0,
        "avg_memory_mb": sum(mem_samples) / len(mem_samples) if mem_samples else 0,
    }


def benchmark_python_collector(duration: int = 10, rate: int = 500):
    with tempfile.NamedTemporaryFile(suffix='.jsonl', delete=False) as f:
        output = f.name

    code = f'''
import asyncio
import json
import time
from pathlib import Path

async def collect(output_path, rate, duration):
    entries = 0
    paths = ["/", "/api", "/admin", "/static"]
    statuses = [200, 200, 200, 301, 400, 404]

    with open(output_path, 'w') as f:
        start = time.time()
        while time.time() - start < duration:
            for _ in range(rate):
                entry = {{
                    "remote_addr": "127.0.0.1",
                    "request": f"GET {{paths[entries % len(paths)]}} HTTP/1.1",
                    "status": statuses[entries % len(statuses)],
                    "response_time": 0.05,
                    "timestamp": time.time()
                }}
                f.write(json.dumps(entry) + "\\n")
                entries += 1
            await asyncio.sleep(1)

asyncio.run(collect("{output}", {rate}, {duration}))
'''

    process = subprocess.Popen(
        ["python3", "-c", code],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )

    cpu_samples, mem_samples = [], []

    def monitor():
        for _ in range(duration):
            try:
                cpu_samples.append(psutil.Process(process.pid).cpu_percent(interval=0.5))
                mem_samples.append(psutil.Process(process.pid).memory_info().rss / 1024 / 1024)
            except:
                pass
            time.sleep(0.5)

    monitor_thread = threading.Thread(target=monitor)
    monitor_thread.start()

    process.wait(timeout=duration + 5)
    monitor_thread.join()

    with open(output, 'r') as f:
        lines = f.readlines()

    Path(output).unlink()

    return {
        "language": "Python",
        "total_entries": len(lines),
        "avg_cpu_percent": sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0,
        "max_cpu_percent": max(cpu_samples) if cpu_samples else 0,
        "avg_memory_mb": sum(mem_samples) / len(mem_samples) if mem_samples else 0,
    }


def main():
    print("=" * 60)
    print("Performance Benchmark: Go vs Python Collector")
    print("=" * 60)

    print("\nRunning Go collector benchmark...")
    go_result = benchmark_go_collector(duration=10, rate=500)

    print("\nRunning Python collector benchmark...")
    py_result = benchmark_python_collector(duration=10, rate=500)

    print("\n" + "=" * 60)
    print("RESULTS")
    print("=" * 60)

    print(f"\n{'Metric':<30} {'Go':<20} {'Python':<20}")
    print("-" * 70)
    print(f"{'Total entries':<30} {go_result['total_entries']:<20} {py_result['total_entries']:<20}")
    print(f"{'Avg CPU (%)':<30} {go_result['avg_cpu_percent']:<20.1f} {py_result['avg_cpu_percent']:<20.1f}")
    print(f"{'Max CPU (%)':<30} {go_result['max_cpu_percent']:<20.1f} {py_result['max_cpu_percent']:<20.1f}")
    print(f"{'Avg Memory (MB)':<30} {go_result['avg_memory_mb']:<20.1f} {py_result['avg_memory_mb']:<20.1f}")

    speedup = py_result['avg_cpu_percent'] / go_result['avg_cpu_percent'] if go_result['avg_cpu_percent'] > 0 else 0
    print(f"\n🚀 Go is {speedup:.1f}x more CPU efficient than Python")


if __name__ == "__main__":
    main()
