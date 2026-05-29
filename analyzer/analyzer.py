#!/usr/bin/env python3
"""Log analyzer using Polars and DuckDB"""

import sys
import time
from pathlib import Path

import polars as pl
import duckdb


def load_logs(file_path: str) -> pl.DataFrame:
    if not Path(file_path).exists():
        raise FileNotFoundError(f"File not found: {file_path}")
    df = pl.read_ndjson(file_path)
    print(f"Loaded {df.height} rows, {df.width} columns")
    print("\nSchema:")
    print(df.schema)
    print("\nFirst 5 rows:")
    print(df.head())
    return df


def clean_data(df: pl.DataFrame) -> pl.DataFrame:
    print("\n--- Cleaning data ---")
    before = df.height
    df = df.unique()
    print(f"Dropped {before - df.height} duplicates")
    df = df.drop_nulls()
    df = df.filter((pl.col("status") >= 100) & (pl.col("status") <= 599))
    df = df.filter((pl.col("response_time") >= 0) & (pl.col("response_time") <= 60))
    print(f"Final rows: {df.height}")
    return df


def aggregate_by_status(df: pl.DataFrame) -> pl.DataFrame:
    result = df.group_by("status").agg([
        pl.col("response_time").mean().alias("avg_rt"),
        pl.col("response_time").min().alias("min_rt"),
        pl.col("response_time").max().alias("max_rt"),
        pl.count().alias("count")
    ]).sort("status")
    print("\n--- Aggregation by status code ---")
    print(result)
    return result


def save_parquet(df: pl.DataFrame, output_path: str):
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(output_path)
    print(f"\nSaved to {output_path}")


def duckdb_analysis(parquet_path: str):
    print("\n--- DuckDB SQL Analysis ---")
    conn = duckdb.connect()
    query = """
    SELECT
        CASE
            WHEN status BETWEEN 200 AND 299 THEN '2xx'
            WHEN status BETWEEN 300 AND 399 THEN '3xx'
            WHEN status BETWEEN 400 AND 499 THEN '4xx'
            WHEN status BETWEEN 500 AND 599 THEN '5xx'
        END as status_group,
        COUNT(*) as request_count,
        AVG(response_time) as avg_response_time,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY response_time) as median_rt,
        COUNT(DISTINCT remote_addr) as unique_ips
    FROM read_parquet(?)
    GROUP BY status_group
    ORDER BY status_group
    """
    result = conn.execute(query, [parquet_path]).fetchdf()
    print(result)
    return result


def compare_performance(df: pl.DataFrame):
    print("\n--- Performance Comparison ---")
    start = time.time()
    df.group_by("status").agg([pl.col("response_time").mean(), pl.count()])
    polars_time = time.time() - start

    start = time.time()
    duckdb.sql("SELECT status, AVG(response_time), COUNT(*) FROM df GROUP BY status")
    duckdb_time = time.time() - start

    print(f"Polars: {polars_time*1000:.2f} ms")
    print(f"DuckDB: {duckdb_time*1000:.2f} ms")


def main():
    input_file = sys.argv[1] if len(sys.argv) > 1 else "data/raw/logs.jsonl"
    parquet_file = "data/processed/logs.parquet"

    df = load_logs(input_file)
    df = clean_data(df)
    aggregate_by_status(df)
    save_parquet(df, parquet_file)
    duckdb_analysis(parquet_file)
    compare_performance(df)


if __name__ == "__main__":
    main()
