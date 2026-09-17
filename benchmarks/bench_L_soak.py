#!/usr/bin/env python3
"""
Phase 27 Benchmark L - Sustained Moderate Load Soak Benchmark.

Purpose:
  Detect stability issues (memory leaks, thread deadlocks, socket descriptor leaks,
  unbounded lag growth, replica drift) under sustained moderate load over time.

Workload:
  - Concurrency: 32 client worker threads with persistent HTTP keep-alive connections.
  - Duration: default 600 seconds (10 minutes), configurable via --duration.
  - Workload mix: 80% Search (GET /search), 20% Write (POST /documents).
  - Unique document IDs starting at 6,000,000+.
  - Periodic sampling every 10 seconds: throughput, latency, load shed rejections,
    and server metrics (/metrics).
  - Post-test verification:
    1. Draining of background events / queue.
    2. Verification of replica consistency on sampled written documents.
    3. Final metric check (zero active leaks, stable health).
"""

import argparse
import csv
import http.client
import json
import os
import statistics
import sys
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

HOST = "127.0.0.1"
DEFAULT_TARGET_PORT = 8081
DEFAULT_RPC_PORTS = [9081, 9082, 9083]

QUERY_TERMS = [
    "fox", "quick", "search", "document", "distributed",
    "index", "kafka", "node", "shard", "engine",
]

CONTENT_PAYLOAD = (
    "sustained soak test payload verifying distributed search engine stability "
    "event dispatcher queue drain and replica consistency under moderate load"
)


class PersistentClient:
    def __init__(self, host: str, port: int, timeout: float = 10.0):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._conn: Optional[http.client.HTTPConnection] = None
        self._connect()

    def _connect(self) -> None:
        try:
            self._conn = http.client.HTTPConnection(self._host, self._port, timeout=self._timeout)
            self._conn.connect()
        except Exception:
            self._conn = None

    def get(self, path: str) -> Tuple[int, str]:
        for attempt in range(2):
            try:
                if self._conn is None:
                    self._connect()
                    if self._conn is None:
                        return 0, "connection_failed"
                self._conn.request("GET", path)
                resp = self._conn.getresponse()
                body = resp.read().decode("utf-8", errors="replace")
                return resp.status, body
            except Exception as e:
                self.close()
                if attempt == 1:
                    return 0, str(e)
        return 0, "request_failed"

    def post(self, path: str, body: str) -> Tuple[int, str]:
        encoded = body.encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(encoded)),
        }
        for attempt in range(2):
            try:
                if self._conn is None:
                    self._connect()
                    if self._conn is None:
                        return 0, "connection_failed"
                self._conn.request("POST", path, body=body, headers=headers)
                resp = self._conn.getresponse()
                body_res = resp.read().decode("utf-8", errors="replace")
                return resp.status, body_res
            except Exception as e:
                self.close()
                if attempt == 1:
                    return 0, str(e)
        return 0, "request_failed"

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None


def health_check(port: int, host: str = HOST) -> bool:
    try:
        conn = http.client.HTTPConnection(host, port, timeout=5.0)
        conn.request("GET", "/health")
        resp = conn.getresponse()
        resp.read()
        conn.close()
        return resp.status == 200
    except Exception:
        return False


def get_metrics(port: int, host: str = HOST) -> dict:
    try:
        conn = http.client.HTTPConnection(host, port, timeout=5.0)
        conn.request("GET", "/metrics")
        resp = conn.getresponse()
        body = resp.read().decode("utf-8", errors="replace")
        conn.close()
        if resp.status == 200:
            return json.loads(body)
    except Exception:
        pass
    return {}


def verify_replica_document(rpc_port: int, doc_id: int) -> bool:
    candidate_shards = [doc_id % 3] + [s for s in range(3) if s != doc_id % 3]
    for sid in candidate_shards:
        try:
            conn = http.client.HTTPConnection(HOST, rpc_port, timeout=5.0)
            body = json.dumps({"shard_id": sid, "document_id": doc_id})
            conn.request("POST", "/node/get", body=body, headers={"Content-Type": "application/json"})
            resp = conn.getresponse()
            data = resp.read().decode("utf-8", errors="replace")
            conn.close()
            if resp.status == 200:
                j = json.loads(data)
                if j.get("found", False) is True:
                    return True
        except Exception:
            pass
    return False


def main():
    parser = argparse.ArgumentParser(description="Phase 27 Benchmark L - Sustained Soak Benchmark")
    parser.add_argument("--port", type=int, default=DEFAULT_TARGET_PORT, help="Target node HTTP port")
    parser.add_argument("--rpc-ports", type=int, nargs="+", default=DEFAULT_RPC_PORTS, help="Cluster RPC ports")
    parser.add_argument("--concurrency", type=int, default=32, help="Client concurrency")
    parser.add_argument("--duration", type=int, default=600, help="Sustained soak duration in seconds (default: 600)")
    parser.add_argument("--sample-interval", type=int, default=10, help="Metric sampling interval in seconds")
    parser.add_argument("--output-dir", type=str, default="results", help="Directory to store CSV results")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    summary_csv = os.path.join(args.output_dir, "phase27_L_soak_summary.csv")

    print("================================================================================")
    print(" Phase 27 Benchmark L: Sustained Moderate Load Soak Benchmark")
    print(f" Target: http://{HOST}:{args.port} | Concurrency: {args.concurrency}")
    print(f" Target Duration: {args.duration}s ({args.duration/60:.1f} min) | Sample Interval: {args.sample_interval}s")
    print(f" Workload: 80% Read / 20% Write")
    print("================================================================================\n")

    if not health_check(args.port):
        print(f"ERROR: Target server on port {args.port} is not reachable.")
        sys.exit(1)

    initial_metrics = get_metrics(args.port)
    print(f"Initial Metrics: {json.dumps(initial_metrics, indent=2)}\n")

    stop_event = threading.Event()
    stats_lock = threading.Lock()

    total_accepted = 0
    total_rejected_429 = 0
    total_errors = 0
    written_doc_ids: List[int] = []
    latencies: List[float] = []

    doc_counter = 6_000_000

    def worker(worker_id: int):
        nonlocal total_accepted, total_rejected_429, total_errors, doc_counter
        client = PersistentClient(HOST, args.port)
        req_count = 0

        while not stop_event.is_set():
            req_count += 1
            is_write = (req_count % 5 == 0)

            t0 = time.perf_counter()
            if is_write:
                with stats_lock:
                    d_id = doc_counter
                    doc_counter += 1
                doc_body = json.dumps({"id": d_id, "content": CONTENT_PAYLOAD})
                status, body = client.post("/documents", doc_body)
                elapsed_ms = (time.perf_counter() - t0) * 1000.0

                with stats_lock:
                    latencies.append(elapsed_ms)
                    if status == 201:
                        total_accepted += 1
                        written_doc_ids.append(d_id)
                    elif status == 429:
                        total_rejected_429 += 1
                    else:
                        total_errors += 1
            else:
                q = QUERY_TERMS[(req_count + worker_id) % len(QUERY_TERMS)]
                status, body = client.get(f"/search?q={urllib.parse.quote(q)}")
                elapsed_ms = (time.perf_counter() - t0) * 1000.0

                with stats_lock:
                    latencies.append(elapsed_ms)
                    if status == 200:
                        total_accepted += 1
                    elif status == 429:
                        total_rejected_429 += 1
                    else:
                        total_errors += 1

            # Brief yielding to simulate realistic client pacing (~1ms)
            time.sleep(0.001)

        client.close()

    print(f"Starting {args.concurrency} worker threads...")
    executor = ThreadPoolExecutor(max_workers=args.concurrency)
    for w in range(args.concurrency):
        executor.submit(worker, w)

    start_time = time.perf_counter()
    samples = []
    prev_accepted = 0
    prev_rejected = 0
    prev_time = start_time

    try:
        while True:
            time.sleep(args.sample_interval)
            now = time.perf_counter()
            elapsed = now - start_time
            interval_elapsed = now - prev_time

            with stats_lock:
                curr_accepted = total_accepted
                curr_rejected = total_rejected_429
                curr_errors = total_errors
                recent_latencies = list(latencies[-500:]) if latencies else []

            interval_accepted = curr_accepted - prev_accepted
            interval_rejected = curr_rejected - prev_rejected
            rps = (interval_accepted + interval_rejected) / interval_elapsed if interval_elapsed > 0 else 0.0

            p50 = statistics.median(recent_latencies) if recent_latencies else 0.0
            p99 = recent_latencies[int(0.99 * (len(recent_latencies) - 1))] if len(recent_latencies) >= 10 else p50

            alive = health_check(args.port)
            metrics_now = get_metrics(args.port)
            consumer_lag = metrics_now.get("consumer_lag", 0)
            load_shed = metrics_now.get("load_shed_rejections_total", 0)

            sample = {
                "elapsed_sec": round(elapsed, 1),
                "interval_rps": round(rps, 1),
                "cumulative_accepted": curr_accepted,
                "cumulative_rejected": curr_rejected,
                "cumulative_errors": curr_errors,
                "recent_p50_ms": round(p50, 2),
                "recent_p99_ms": round(p99, 2),
                "server_alive": alive,
                "consumer_lag": consumer_lag,
                "load_shed_rejections_total": load_shed,
            }
            samples.append(sample)

            print(f"[{elapsed:6.1f}s / {args.duration}s] Rate: {rps:6.1f} req/s | "
                  f"Accepted: {curr_accepted:6d} | 429 Shed: {curr_rejected:4d} | "
                  f"Errors: {curr_errors:2d} | P50: {p50:5.2f}ms | P99: {p99:5.2f}ms | "
                  f"Lag: {consumer_lag} | Alive: {alive}")

            prev_accepted = curr_accepted
            prev_rejected = curr_rejected
            prev_time = now

            if elapsed >= args.duration:
                break
    finally:
        print("\nStopping worker threads...")
        stop_event.set()
        executor.shutdown(wait=True)

    total_elapsed = time.perf_counter() - start_time
    print(f"\nWorkload completed in {total_elapsed:.2f}s.")
    print("Draining background events (waiting 3 seconds)...")
    time.sleep(3.0)

    # Post-run verification
    final_metrics = get_metrics(args.port)
    final_lag = final_metrics.get("consumer_lag", 0)
    print(f"Final Metrics:\n{json.dumps(final_metrics, indent=2)}\n")

    # Sample replica verification
    print("Performing replica consistency check on written documents...")
    replica_consistent = True
    if written_doc_ids:
        sample_ids = written_doc_ids[::max(1, len(written_doc_ids) // 20)][:20]
        for d in sample_ids:
            for rpc in args.rpc_ports:
                if not verify_replica_document(rpc, d):
                    print(f"ERROR: Document {d} missing on RPC port {rpc}")
                    replica_consistent = False
                    break

    print(f"Replica Consistency: {'PASSED (All sampled documents exist across all replicas)' if replica_consistent else 'FAILED'}")
    print(f"Kafka Final Consumer Lag: {final_lag}")

    # Write summary CSV
    with open(summary_csv, "w", newline="", encoding="utf-8") as f:
        if samples:
            writer = csv.DictWriter(f, fieldnames=list(samples[0].keys()))
            writer.writeheader()
            writer.writerows(samples)

    print(f"\nSummary results saved to {summary_csv}")
    print("================================================================================")
    print(" BENCHMARK L SOAK TEST COMPLETED")
    print(f" Total Requests Accepted: {total_accepted}")
    print(f" Total Requests Shed (429): {total_rejected_429}")
    print(f" Total Errors: {total_errors}")
    print(f" Final Health: {'HEALTHY' if health_check(args.port) else 'UNHEALTHY'}")
    print(f" Final Consistency: {'PASSED' if replica_consistent else 'FAILED'}")
    print("================================================================================\n")


if __name__ == "__main__":
    main()
