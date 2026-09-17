#!/usr/bin/env python3
"""
Phase 27 Benchmark K - Edge Load Management & High-Concurrency Stress Benchmark.

Purpose:
  Demonstrate graceful server behavior when offered concurrency exceeds the
  configured application-level edge concurrency limit (DSE_MAX_CONCURRENT_REQUESTS).

Architecture under test:
  - 3 real nodes, S=3, R=3 synchronous replication.
  - Client sends requests to Node 0 (HttpServer).
  - HttpServer enforces max_concurrent_requests (default 64) via zero-lock atomic admission.
  - NodeServer RPCs remain unconstrained for internal replication.
  - Excess requests receive HTTP 429 Too Many Requests without causing application state changes.
  - /health and /metrics remain accessible throughout overload.

Progression:
  Concurrency: 16 -> 32 -> 64 -> 128 -> 256
  Workload: 80% Search (GET /search), 20% Write (POST /documents)
  Unique document IDs per tier: starting at 5,000,000+
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
from concurrent.futures import ThreadPoolExecutor, as_completed
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
    "distributed search engine stress test payload verifying application "
    "level edge concurrency limiting load shedding and graceful degradation "
    "under extreme client saturation without unbounded queue latency spikes"
)

CONCURRENCY_TIERS = [16, 32, 64, 128, 256]


@dataclass
class RequestRecord:
    concurrency: int
    req_id: int
    op_type: str        # "search" or "write"
    status_code: int    # 200, 201, 429, or 0 (connection error)
    latency_ms: float
    accepted: bool
    rejected_429: bool
    error: str = ""


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


def run_tier(
    target_port: int,
    concurrency: int,
    requests_per_worker: int,
    start_doc_id: int,
) -> Tuple[List[RequestRecord], List[int]]:
    records: List[RequestRecord] = []
    written_doc_ids: List[int] = []
    write_lock = threading.Lock()
    start_barrier = threading.Barrier(concurrency)

    def worker(worker_id: int) -> List[RequestRecord]:
        client = PersistentClient(HOST, target_port)
        worker_records = []
        doc_counter = start_doc_id + worker_id * requests_per_worker

        try:
            start_barrier.wait(timeout=15.0)
        except Exception:
            pass

        for i in range(requests_per_worker):
            req_idx = worker_id * requests_per_worker + i
            # 80% search, 20% write
            is_write = (i % 5 == 0)

            t0 = time.perf_counter()
            if is_write:
                d_id = doc_counter
                doc_counter += 1
                doc_body = json.dumps({"id": d_id, "content": CONTENT_PAYLOAD})
                status, body = client.post("/documents", doc_body)
                elapsed_ms = (time.perf_counter() - t0) * 1000.0

                accepted = (status == 201)
                rejected = (status == 429)
                err = "" if (accepted or rejected) else f"HTTP_{status}: {body[:60]}"
                if accepted:
                    with write_lock:
                        written_doc_ids.append(d_id)

                worker_records.append(
                    RequestRecord(concurrency, req_idx, "write", status, elapsed_ms, accepted, rejected, err)
                )
            else:
                q = QUERY_TERMS[(req_idx + worker_id) % len(QUERY_TERMS)]
                status, body = client.get(f"/search?q={urllib.parse.quote(q)}")
                elapsed_ms = (time.perf_counter() - t0) * 1000.0

                accepted = (status == 200)
                rejected = (status == 429)
                err = "" if (accepted or rejected) else f"HTTP_{status}: {body[:60]}"

                worker_records.append(
                    RequestRecord(concurrency, req_idx, "search", status, elapsed_ms, accepted, rejected, err)
                )

        client.close()
        return worker_records

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker, w) for w in range(concurrency)]
        for f in as_completed(futures):
            records.extend(f.result())

    return records, written_doc_ids


def main():
    parser = argparse.ArgumentParser(description="Phase 27 Benchmark K - Stress Benchmark")
    parser.add_argument("--port", type=int, default=DEFAULT_TARGET_PORT, help="Target node port (Node 0)")
    parser.add_argument("--rpc-ports", type=int, nargs="+", default=DEFAULT_RPC_PORTS, help="Cluster RPC ports")
    parser.add_argument("--requests-per-tier", type=int, default=1000, help="Total requests per concurrency tier")
    parser.add_argument("--output-dir", type=str, default="results", help="Directory to store CSV results")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    summary_csv = os.path.join(args.output_dir, "phase27_K_stress_summary.csv")
    requests_csv = os.path.join(args.output_dir, "phase27_K_stress_requests.csv")

    print("================================================================================")
    print(" Phase 27 Benchmark K: Edge Load Management & High-Concurrency Stress Benchmark")
    print(f" Target: http://{HOST}:{args.port} | Tiers: {CONCURRENCY_TIERS}")
    print(f" Workload: 80% Read / 20% Write | Target requests per tier: ~{args.requests_per_tier}")
    print("================================================================================\n")

    if not health_check(args.port):
        print(f"ERROR: Server on port {args.port} is not reachable.")
        sys.exit(1)

    initial_metrics = get_metrics(args.port)
    initial_load_shed = initial_metrics.get("load_shed_rejections_total", 0)
    print(f"Initial server load_shed_rejections_total: {initial_load_shed}\n")

    all_records: List[RequestRecord] = []
    tier_summaries = []

    doc_id_base = 5_000_000

    for c in CONCURRENCY_TIERS:
        reqs_per_worker = max(1, args.requests_per_tier // c)
        actual_total = reqs_per_worker * c
        print(f"--- Concurrency Tier c={c} ({c} workers x {reqs_per_worker} reqs = {actual_total} total) ---")

        t_start = time.perf_counter()
        records, written_docs = run_tier(args.port, c, reqs_per_worker, doc_id_base)
        t_elapsed = time.perf_counter() - t_start

        all_records.extend(records)
        doc_id_base += actual_total + 1000

        # Tally results
        accepted = [r for r in records if r.accepted]
        rejected_429 = [r for r in records if r.rejected_429]
        other_errors = [r for r in records if not r.accepted and not r.rejected_429]

        acc_latencies = [r.latency_ms for r in accepted]
        if acc_latencies:
            acc_latencies.sort()
            p50 = statistics.median(acc_latencies)
            p95 = acc_latencies[int(0.95 * (len(acc_latencies) - 1))]
            p99 = acc_latencies[int(0.99 * (len(acc_latencies) - 1))]
            mean_lat = statistics.mean(acc_latencies)
        else:
            p50 = p95 = p99 = mean_lat = 0.0

        throughput = len(records) / t_elapsed if t_elapsed > 0 else 0.0
        accepted_throughput = len(accepted) / t_elapsed if t_elapsed > 0 else 0.0
        rej_pct = (len(rejected_429) / len(records) * 100.0) if records else 0.0

        # Health check
        is_healthy = health_check(args.port)
        metrics_now = get_metrics(args.port)
        cur_load_shed = metrics_now.get("load_shed_rejections_total", 0)

        # Replica consistency check on sampled writes (up to 5 sampled docs)
        replica_ok = True
        if written_docs:
            sample_docs = written_docs[:5]
            for d in sample_docs:
                for rpc in args.rpc_ports:
                    if not verify_replica_document(rpc, d):
                        replica_ok = False
                        break

        tier_summary = {
            "concurrency": c,
            "total_requests": len(records),
            "accepted_count": len(accepted),
            "rejected_429_count": len(rejected_429),
            "rejected_percent": round(rej_pct, 2),
            "other_errors_count": len(other_errors),
            "elapsed_sec": round(t_elapsed, 3),
            "total_throughput_rps": round(throughput, 2),
            "accepted_throughput_rps": round(accepted_throughput, 2),
            "accepted_p50_ms": round(p50, 2),
            "accepted_p95_ms": round(p95, 2),
            "accepted_p99_ms": round(p99, 2),
            "accepted_mean_ms": round(mean_lat, 2),
            "server_healthy": is_healthy,
            "load_shed_metric": cur_load_shed,
            "replica_consistent": replica_ok,
        }
        tier_summaries.append(tier_summary)

        print(f"  Accepted:    {len(accepted)} ({round(len(accepted)/len(records)*100, 1)}%)")
        print(f"  Shed (429):  {len(rejected_429)} ({round(rej_pct, 1)}%)")
        print(f"  Errors:      {len(other_errors)}")
        print(f"  Duration:    {round(t_elapsed, 2)}s | Total Throughput: {round(throughput, 1)} rps")
        print(f"  Accepted Latency: P50={round(p50, 2)}ms, P95={round(p95, 2)}ms, P99={round(p99, 2)}ms")
        print(f"  Server Alive: {is_healthy} | Server load_shed_metric: {cur_load_shed}")
        print(f"  Replica Consistency: {'PASSED' if replica_ok else 'FAILED'}\n")

    # Write summary CSV
    print(f"Writing summary to {summary_csv}...")
    fieldnames = list(tier_summaries[0].keys())
    with open(summary_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(tier_summaries)

    # Write request records CSV
    print(f"Writing detailed records to {requests_csv}...")
    with open(requests_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["concurrency", "req_id", "op_type", "status_code", "latency_ms", "accepted", "rejected_429", "error"])
        for r in all_records:
            writer.writerow([r.concurrency, r.req_id, r.op_type, r.status_code, round(r.latency_ms, 3), int(r.accepted), int(r.rejected_429), r.error])

    print("\n================================================================================")
    print(" BENCHMARK K SUMMARY RESULTS")
    print("================================================================================")
    header_fmt = "{:<12} {:<10} {:<12} {:<10} {:<12} {:<12} {:<12}"
    row_fmt    = "{:<12} {:<10} {:<12} {:<10} {:<12.2f} {:<12.2f} {:<12.2f}"
    print(header_fmt.format("Concurrency", "Accepted", "Shed (429)", "Shed %", "P50 (ms)", "P99 (ms)", "Throughput"))
    print("-" * 82)
    for s in tier_summaries:
        print(row_fmt.format(
            s["concurrency"],
            s["accepted_count"],
            s["rejected_429_count"],
            f"{s['rejected_percent']}%",
            s["accepted_p50_ms"],
            s["accepted_p99_ms"],
            s["total_throughput_rps"],
        ))
    print("================================================================================\n")


if __name__ == "__main__":
    main()
