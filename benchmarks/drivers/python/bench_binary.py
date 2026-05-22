"""Benchmark MongoCore Python client via Binary UDS transport."""

import asyncio
import json
import os
import platform
import sys
import time
import statistics
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent.parent / "clients" / "python" / "src"))
from mongocore.binary_transport import BinaryTransport, BinaryTransportPool

CONFIG = json.loads((Path(__file__).parent.parent / "common.json").read_text())
DATA_DIR = Path(__file__).parent.parent.parent / "data"
RESULTS_DIR = Path(__file__).parent.parent.parent / "results"
RESULTS_DIR.mkdir(exist_ok=True)

QUICK_MODE = "--quick" in sys.argv

WARMUP = 0 if QUICK_MODE else CONFIG["warmup_iterations"]["python"]
MIN_TIME = 0 if QUICK_MODE else CONFIG["min_time_secs"]
MAX_ITERS = 1 if QUICK_MODE else CONFIG["max_iterations"]
MAX_TIME = 5 if QUICK_MODE else CONFIG["max_time_secs"]
DB_NAME = CONFIG["database"]
BINARY_SOCKET = CONFIG.get("mongocore_binary_socket_path", "/tmp/mongocore.bin.sock")


def get_system_info():
    return {
        "os": platform.system().lower(),
        "arch": platform.machine(),
        "cpus": os.cpu_count(),
        "mongocore_version": "0.6.0",
        "driver": "mongocore+python+binary",
        "transport": "binary_uds",
    }


def run_benchmark(name, category, setup_fn, before_task_fn, task_fn, after_task_fn, teardown_fn, dataset_size_bytes, batch_size=1):
    """Run a benchmark following MongoDB spec methodology."""
    transport = BinaryTransport(BINARY_SOCKET)
    transport.connect()

    setup_fn(transport)

    # Warmup
    for _ in range(WARMUP):
        before_task_fn(transport)
        task_fn(transport)
        after_task_fn(transport)

    # Timed iterations
    times = []
    total_time = 0.0
    iteration = 0

    while total_time < MIN_TIME or iteration < 5:
        if iteration >= MAX_ITERS or total_time >= MAX_TIME:
            break

        before_task_fn(transport)

        start = time.perf_counter()
        task_fn(transport)
        elapsed = time.perf_counter() - start

        after_task_fn(transport)

        times.append(elapsed)
        total_time += elapsed
        iteration += 1

    teardown_fn(transport)
    transport.close()

    times.sort()
    median = statistics.median(times)
    ops_per_sec = batch_size / median
    mb_per_sec = dataset_size_bytes / median / 1_000_000

    def percentile(data, pct):
        import math
        idx = max(0, math.ceil(len(data) * pct / 100) - 1)
        return data[min(idx, len(data) - 1)]

    result = {
        "benchmark": name,
        "category": category,
        "driver": "mongocore+python+binary",
        "dataset_size_bytes": dataset_size_bytes,
        "batch_size": batch_size,
        "iterations": len(times),
        "total_time_secs": round(total_time, 3),
        "ops_per_sec": round(ops_per_sec, 1),
        "mb_per_sec": round(mb_per_sec, 3),
        "percentiles": {
            "p10": round(percentile(times, 10), 6),
            "p25": round(percentile(times, 25), 6),
            "p50": round(median, 6),
            "p75": round(percentile(times, 75), 6),
            "p90": round(percentile(times, 90), 6),
            "p95": round(percentile(times, 95), 6),
            "p99": round(percentile(times, 99), 6),
        },
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "system": get_system_info(),
    }
    print(f"  {name}: {ops_per_sec:.0f} ops/s, {mb_per_sec:.2f} MB/s ({len(times)} iters)")
    return result


def main():
    if not BinaryTransport.is_available(BINARY_SOCKET):
        print(f"ERROR: Binary transport socket not found at {BINARY_SOCKET}")
        print("Start MongoCore with binary transport enabled first.")
        sys.exit(1)

    print(f"=== MongoCore+Python Binary UDS benchmarks ===")
    print(f"Socket: {BINARY_SOCKET}")
    results = []

    small_doc = json.loads((DATA_DIR / "small_doc.json").read_text())
    tweet_doc = json.loads((DATA_DIR / "tweet.json").read_text())
    large_doc = json.loads((DATA_DIR / "large_doc.json").read_text())
    small_size = len(json.dumps(small_doc).encode())
    tweet_size = len(json.dumps(tweet_doc).encode())
    large_size = len(json.dumps(large_doc).encode())

    # Run Command (batch 10,000 hello commands per iteration)
    def task_run_command(t):
        for _ in range(10_000):
            t.run_command(DB_NAME, {"hello": 1})

    results.append(run_benchmark(
        "run_command", "single_doc",
        setup_fn=lambda t: None,
        before_task_fn=lambda t: None,
        task_fn=task_run_command,
        after_task_fn=lambda t: None,
        teardown_fn=lambda t: None,
        dataset_size_bytes=10_000 * 100, batch_size=10_000,
    ))

    # Find One by ID (batch 10,000 finds per iteration)
    def setup_find(t):
        t.run_command(DB_NAME, {"drop": "bench_find_bin"})
        t.insert_one(DB_NAME, "bench_find_bin", {"_id": "bench_find_001", **tweet_doc})

    def task_find(t):
        for _ in range(10_000):
            t.find_one(DB_NAME, "bench_find_bin", {"_id": "bench_find_001"})

    def teardown_find(t):
        t.run_command(DB_NAME, {"drop": "bench_find_bin"})

    results.append(run_benchmark(
        "find_one_by_id", "single_doc",
        setup_fn=setup_find,
        before_task_fn=lambda t: None,
        task_fn=task_find,
        after_task_fn=lambda t: None,
        teardown_fn=teardown_find,
        dataset_size_bytes=10_000 * tweet_size, batch_size=10_000,
    ))

    # InsertOne Small (batch 10,000 inserts per iteration)
    def task_insert_small(t):
        from bson import ObjectId
        for _ in range(10_000):
            t.insert_one(DB_NAME, "bench_insert_small_bin", {**small_doc, "_id": str(ObjectId())})

    def before_insert_small(t):
        t.run_command(DB_NAME, {"drop": "bench_insert_small_bin"})

    results.append(run_benchmark(
        "insert_one_small", "single_doc",
        setup_fn=lambda t: None,
        before_task_fn=before_insert_small,
        task_fn=task_insert_small,
        after_task_fn=lambda t: None,
        teardown_fn=lambda t: None,
        dataset_size_bytes=10_000 * small_size, batch_size=10_000,
    ))

    # InsertOne Large (batch 10 inserts per iteration)
    def task_insert_large(t):
        from bson import ObjectId
        for _ in range(10):
            t.insert_one(DB_NAME, "bench_insert_large_bin", {**large_doc, "_id": str(ObjectId())})

    def before_insert_large(t):
        t.run_command(DB_NAME, {"drop": "bench_insert_large_bin"})

    results.append(run_benchmark(
        "insert_one_large", "single_doc",
        setup_fn=lambda t: None,
        before_task_fn=before_insert_large,
        task_fn=task_insert_large,
        after_task_fn=lambda t: None,
        teardown_fn=lambda t: None,
        dataset_size_bytes=10 * large_size, batch_size=10,
    ))

    # Bulk Insert Small (10K per iteration)
    def task_bulk(t):
        from bson import ObjectId
        docs = [{**small_doc, "_id": str(ObjectId())} for _ in range(10_000)]
        t.insert_many(DB_NAME, "bench_bulk_bin", docs)

    def before_bulk(t):
        t.run_command(DB_NAME, {"drop": "bench_bulk_bin"})

    results.append(run_benchmark(
        "bulk_insert_small", "multi_doc",
        setup_fn=lambda t: None,
        before_task_fn=before_bulk,
        task_fn=task_bulk,
        after_task_fn=lambda t: None,
        teardown_fn=lambda t: None,
        dataset_size_bytes=small_size * 10_000, batch_size=10_000,
    ))

    # Bulk Insert Large (10 x 2.75MB docs per iteration)
    def task_bulk_large(t):
        from bson import ObjectId
        docs = [{**large_doc, "_id": str(ObjectId())} for _ in range(10)]
        t.insert_many(DB_NAME, "bench_bulk_large_bin", docs)

    def before_bulk_large(t):
        t.run_command(DB_NAME, {"drop": "bench_bulk_large_bin"})

    results.append(run_benchmark(
        "bulk_insert_large", "multi_doc",
        setup_fn=lambda t: None,
        before_task_fn=before_bulk_large,
        task_fn=task_bulk_large,
        after_task_fn=lambda t: None,
        teardown_fn=lambda t: None,
        dataset_size_bytes=large_size * 10, batch_size=10,
    ))

    # Find Many (10,000 small docs)
    def setup_find_many(t):
        from bson import ObjectId
        t.run_command(DB_NAME, {"drop": "bench_find_many_bin"})
        docs = [{**small_doc, "_id": str(ObjectId())} for _ in range(10_000)]
        t.insert_many(DB_NAME, "bench_find_many_bin", docs)

    def task_find_many(t):
        t.find(DB_NAME, "bench_find_many_bin")

    def teardown_find_many(t):
        t.run_command(DB_NAME, {"drop": "bench_find_many_bin"})

    results.append(run_benchmark(
        "find_many", "multi_doc",
        setup_fn=setup_find_many,
        before_task_fn=lambda t: None,
        task_fn=task_find_many,
        after_task_fn=lambda t: None,
        teardown_fn=teardown_find_many,
        dataset_size_bytes=small_size * 10_000, batch_size=10_000,
    ))

    # Find Many Large (10 x ~2.75MB docs)
    def setup_find_many_large(t):
        from bson import ObjectId
        t.run_command(DB_NAME, {"drop": "bench_find_many_large_bin"})
        docs = [{**large_doc, "_id": str(ObjectId())} for _ in range(10)]
        t.insert_many(DB_NAME, "bench_find_many_large_bin", docs)

    def task_find_many_large(t):
        t.find(DB_NAME, "bench_find_many_large_bin")

    def teardown_find_many_large(t):
        t.run_command(DB_NAME, {"drop": "bench_find_many_large_bin"})

    results.append(run_benchmark(
        "find_many_large", "multi_doc",
        setup_fn=setup_find_many_large,
        before_task_fn=lambda t: None,
        task_fn=task_find_many_large,
        after_task_fn=lambda t: None,
        teardown_fn=teardown_find_many_large,
        dataset_size_bytes=large_size * 10, batch_size=10,
    ))

    # Save results
    output_path = RESULTS_DIR / "python_binary.json"
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2)
    print(f"\nResults saved to {output_path}")


if __name__ == "__main__":
    main()
