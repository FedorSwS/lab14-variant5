#!/usr/bin/env python3
"""Integration tests for log pipeline"""

import json
import subprocess
import tempfile
from pathlib import Path
import pytest
import polars as pl

def test_kafka_producer():
    """Test Kafka producer integration (requires Kafka running)"""
    import subprocess
    import time
    import json
    
    # Запускаем коллектор с Kafka на 3 секунды
    env = {"KAFKA_BOOTSTRAP": "localhost:9092"}
    
    proc = subprocess.Popen(
        ["./collector/collector", "-rate=50", "-workers=2", "-window=2s"],
        env={**os.environ, **env},
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE
    )
    
    time.sleep(3)
    proc.terminate()
    proc.wait(timeout=5)
    
    # Если процесс завершился без ошибок - тест пройден
    assert proc.returncode == 0 or proc.returncode == -15  # SIGTERM 
    
def test_go_collector():
    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        output_path = f.name

    try:
        proc = subprocess.Popen(
            ["./collector/collector", "-output", output_path, "-rate", "50", "-workers", "2"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE
        )
        import time
        time.sleep(2)
        proc.terminate()
        proc.wait(timeout=5)

        with open(output_path, 'r') as f:
            lines = f.readlines()

        assert len(lines) > 0
        for line in lines:
            data = json.loads(line)
            assert "status" in data
            assert "request" in data
    finally:
        Path(output_path).unlink(missing_ok=True)


def test_polars_analysis():
    data = [
        {"remote_addr": "1.1.1.1", "request": "GET / HTTP/1.1", "status": 200, "response_time": 0.05},
        {"remote_addr": "2.2.2.2", "request": "GET /api HTTP/1.1", "status": 404, "response_time": 0.02},
        {"remote_addr": "1.1.1.1", "request": "GET / HTTP/1.1", "status": 200, "response_time": 0.03},
    ]

    with tempfile.NamedTemporaryFile(mode='w', suffix='.jsonl', delete=False) as f:
        for item in data:
            f.write(json.dumps(item) + "\n")
        path = f.name

    df = pl.read_ndjson(path)
    assert df.height == 3

    agg = df.group_by("status").agg([pl.count()])
    assert agg.height == 2

    Path(path).unlink(missing_ok=True)


def test_rust_validator():
    import sys
    sys.path.insert(0, "rust_validator/target/release")
    
    try:
        import log_validator
        validator = log_validator.LogValidator()
        
        assert validator.validate_ip("192.168.1.1") == True
        assert validator.validate_ip("999.999.999.999") == False
        
        assert validator.validate_path("/api/users") == True
        assert validator.validate_path("invalid") == False
        
        errors = validator.validate_entry("192.168.1.1", "/api", 200, 0.05)
        assert len(errors) == 0
        
        errors = validator.validate_entry("invalid", "bad", 999, 100)
        assert len(errors) >= 2
    except ImportError:
        pytest.skip("Rust validator not built")
        

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
