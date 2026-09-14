#!/usr/bin/env python3
"""
Phase 21 Benchmark C — 3-Node Indexing.

Indexes documents via Node 0 (port 8081) across a 3-node cluster
with synchronous replication (N=3, R=3, S=3).

Per concurrency level:
  - Warmup: 100 POST requests (discarded)
  - Measured: 500 POST requests
  - Unique document ID ranges per level (no reuse)

ID ranges:
  warmup  → per-level range below the measured range
  C1  → 500001–500500   warmup: 490001–490100
  C2  → 501001–501500   warmup: 491001–491100
  C4  → 502001–502500   warmup: 492001–492100
  C8  → 503001–503500   warmup: 493001–493100
  C16 → 504001–504500   warmup: 494001–494100
"""

import argparse
import csv
import http.client
import json
import os
import statistics
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from threading import Lock
from typing import List, Tuple

CONTENT_M = (
    "distributed search engines index large document collections using "
    "inverted index structures with tf-idf scoring algorithms for ranked "
    "retrieval supporting boolean query modes including or and and with "
    "concurrent multi-shard fan-out across replicated node clusters"
)

NODE0_HTTP = 8081
HOST = "127.0.0.1"

CONCURRENCY_CONFIG = {
    1:  {"measured_start": 500_001, "warmup_start": 490_001},
    2:  {"measured_start": 501_001, "warmup_start": 491_001},
    4:  {"measured_start": 502_001, "warmup_start": 492_001},
    8:  {"measured_start": 503_001, "warmup_start": 493_001},
    16: {"measured_start": 504_001, "warmup_start": 494_001},
}


@dataclass
class Record:
    request_id: int
    doc_id: int
    status_code: int
    success: bool
    latency_ms: float
    error: str = ""


class Conn:
    def __init__(self, port: int):
        self._port = port
        self._conn = http.client.HTTPConnection(HOST, port, timeout=30.0)
        self._conn.connect()

    def post(self, path: str, body: str) -> Tuple[int, str]:
        encoded = body.encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(encoded)),
        }
        try:
            self._conn.request("POST", path, body=body, headers=headers)
            resp = self._conn.getresponse()
            rb = resp.read().decode("utf-8", errors="replace")
            return resp.status, rb
        except Exception:
            self._conn.close()
            self._conn = http.client.HTTPConnection(HOST, self._port, timeout=30.0)
            self._conn.connect()
            raise

    def close(self):
        self._conn.close()


def health_check(port: int) -> bool:
    try:
        conn = http.client.HTTPConnection(HOST, port, timeout=5.0)
        conn.request("GET", "/health")
        resp = conn.getresponse()
        resp.read()
        conn.close()
        return resp.status == 200
    except Exception:
        return False


def wait_healthy(port: int, timeout_s: float = 20.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if health_check(port):
            return True
        time.sleep(0.2)
    return False


def get_metrics(port: int) -> dict:
    try:
        conn = http.client.HTTPConnection(HOST, port, timeout=5.0)
        conn.request("GET", "/metrics")
        resp = conn.getresponse()
        body = resp.read().decode("utf-8", errors="replace")
        conn.close()
        if resp.status == 200:
            return json.loads(body)
    except Exception:
        pass
    return {}


# ---------------------------------------------------------------------------
# Percentile calculation (exact Phase 21 methodology from benchmarks/bench.py)
# ---------------------------------------------------------------------------

def compute_percentiles(samples: list[float]) -> tuple[float, float, float, float]:
    """Return (p50, p95, p99, mean) over the given latency samples (ms).

    Convention: statistics.quantiles(data, n=100, method="inclusive").
    qs[k-1] is the k-th percentile (1 <= k <= 99).
      p50  → qs[49]
      p95  → qs[94]
      p99  → qs[98]

    Handles degenerate cases (0 or 1 samples) safely.
    """
    if not samples:
        return 0.0, 0.0, 0.0, 0.0
    n = len(samples)
    mean = statistics.mean(samples)
    if n == 1:
        v = samples[0]
        return v, v, v, mean
    try:
        qs = statistics.quantiles(samples, n=100, method="inclusive")
        return qs[49], qs[94], qs[98], mean
    except statistics.StatisticsError:
        # Fallback: sorted-array direct indexing
        s = sorted(samples)
        p50 = s[max(0, int(0.50 * n) - 1)]
        p95 = s[max(0, int(0.95 * n) - 1)]
        p99 = s[max(0, int(0.99 * n) - 1)]
        return p50, p95, p99, mean


def worker(
    doc_ids: List[int],
    records_out: List[Record],
    records_lock: Lock,
    is_warmup: bool,
    port: int,
) -> None:
    conn = Conn(port)
    try:
        for req_id, doc_id in enumerate(doc_ids):
            body = json.dumps({"id": doc_id, "content": CONTENT_M})
            status = 0
            error_str = ""
            success = False

            t0 = time.perf_counter()
            try:
                status, resp_body = conn.post("/documents", body)
                success = (status == 201)
                if not success:
                    error_str = f"HTTP {status}: {resp_body[:100]}"
            except Exception as exc:
                error_str = str(exc)
            t1 = time.perf_counter()
            latency_ms = (t1 - t0) * 1000.0

            if not is_warmup:
                rec = Record(
                    request_id=req_id,
                    doc_id=doc_id,
                    status_code=status,
                    success=success,
                    latency_ms=round(latency_ms, 4),
                    error=error_str,
                )
                with records_lock:
                    records_out.append(rec)
    finally:
        conn.close()


def run_benchmark(
    concurrency: int,
    warmup_count: int,
    measured_count: int,
    run_id: str,
    out_prefix: str,
) -> Tuple[dict, List[Record]]:
    cfg = CONCURRENCY_CONFIG[concurrency]
    measured_ids = list(range(cfg["measured_start"], cfg["measured_start"] + measured_count))
    warmup_ids   = list(range(cfg["warmup_start"],   cfg["warmup_start"]   + warmup_count))

    print(f"\n  --- Concurrency={concurrency} ---")
    print(f"  Warmup IDs:   {warmup_ids[0]}–{warmup_ids[-1]}")
    print(f"  Measured IDs: {measured_ids[0]}–{measured_ids[-1]}")

    def split_chunks(ids, n):
        size = max(1, len(ids) // n)
        chunks = [ids[i:i+size] for i in range(0, len(ids), size)]
        return chunks

    # Warmup
    print(f"  Running {warmup_count} warmup requests (discarded)...")
    warmup_lock = Lock()
    warmup_records: List[Record] = []
    warmup_chunks = split_chunks(warmup_ids, concurrency)
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [ex.submit(worker, ch, warmup_records, warmup_lock, True, NODE0_HTTP)
                for ch in warmup_chunks]
        for f in as_completed(futs): f.result()
    print(f"  Warmup complete ({warmup_count} requests discarded).")

    # Metrics BEFORE
    metrics_before = get_metrics(NODE0_HTTP)
    wt_before = metrics_before.get("coordinator_writes_total", 0)
    ws_before = metrics_before.get("coordinator_write_success", 0)
    we_before = metrics_before.get("coordinator_write_errors", 0)
    et_before = metrics_before.get("events_total", 0)
    ep_before = metrics_before.get("events_published", 0)
    ef_before = metrics_before.get("events_failed", 0)
    print(f"  Metrics BEFORE: writes_total={wt_before} write_success={ws_before} write_errors={we_before}")
    print(f"                  events_total={et_before} events_published={ep_before} events_failed={ef_before}")

    # Measured run
    print(f"  Running {measured_count} measured requests at concurrency={concurrency}...")
    records: List[Record] = []
    records_lock = Lock()
    meas_chunks = split_chunks(measured_ids, concurrency)

    t_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [ex.submit(worker, ch, records, records_lock, False, NODE0_HTTP)
                for ch in meas_chunks]
        for f in as_completed(futs): f.result()
    t_end = time.perf_counter()

    elapsed_s = t_end - t_start

    # Metrics AFTER
    metrics_after = get_metrics(NODE0_HTTP)
    wt_after = metrics_after.get("coordinator_writes_total", 0)
    ws_after = metrics_after.get("coordinator_write_success", 0)
    we_after = metrics_after.get("coordinator_write_errors", 0)
    et_after = metrics_after.get("events_total", 0)
    ep_after = metrics_after.get("events_published", 0)
    ef_after = metrics_after.get("events_failed", 0)
    print(f"  Metrics AFTER:  writes_total={wt_after} write_success={ws_after} write_errors={we_after}")
    print(f"                  events_total={et_after} events_published={ep_after} events_failed={ef_after}")
    print(f"  Deltas:         writes_total={wt_after-wt_before} write_success={ws_after-ws_before} write_errors={we_after-we_before}")
    print(f"                  events_total={et_after-et_before} events_published={ep_after-ep_before} events_failed={ef_after-ef_before}")

    # Stats
    records.sort(key=lambda r: r.request_id)
    total_requests = len(records)
    successful = sum(1 for r in records if r.success)
    errors = total_requests - successful
    latencies = sorted([r.latency_ms for r in records if r.success])

    if latencies:
        p50, p95, p99, mean_ms = compute_percentiles(latencies)
    else:
        p50 = p95 = p99 = mean_ms = 0.0

    throughput = total_requests / elapsed_s if elapsed_s > 0 else 0.0

    summary = {
        "run_id": run_id,
        "concurrency": concurrency,
        "total_requests": total_requests,
        "successful": successful,
        "errors": errors,
        "p50_ms": round(p50, 4),
        "p95_ms": round(p95, 4),
        "p99_ms": round(p99, 4),
        "mean_ms": round(mean_ms, 4),
        "elapsed_s": round(elapsed_s, 4),
        "throughput_req_s": round(throughput, 4),
        "metrics_before": metrics_before,
        "metrics_after": metrics_after,
        "delta_writes_total": wt_after - wt_before,
        "delta_write_success": ws_after - ws_before,
        "delta_write_errors": we_after - we_before,
        "delta_events_total": et_after - et_before,
        "delta_events_published": ep_after - ep_before,
        "delta_events_failed": ef_after - ef_before,
    }

    # Write request CSV
    os.makedirs(os.path.dirname(out_prefix) if os.path.dirname(out_prefix) else ".", exist_ok=True)
    req_csv = out_prefix + "_requests.csv"
    with open(req_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["run_id", "concurrency", "request_id", "doc_id",
                         "status_code", "success", "latency_ms", "error"])
        for r in records:
            writer.writerow([run_id, concurrency, r.request_id, r.doc_id,
                             r.status_code, r.success, r.latency_ms, r.error])

    # Write summary CSV
    sum_csv = out_prefix + "_summary.csv"
    with open(sum_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["run_id","concurrency","requests","successful","errors",
                         "p50_ms","p95_ms","p99_ms","mean_ms","elapsed_s","throughput_req_s",
                         "delta_writes_total","delta_write_success","delta_write_errors",
                         "delta_events_total","delta_events_published","delta_events_failed"])
        writer.writerow([run_id, concurrency, total_requests, successful, errors,
                         summary["p50_ms"], summary["p95_ms"], summary["p99_ms"],
                         summary["mean_ms"], summary["elapsed_s"], summary["throughput_req_s"],
                         summary["delta_writes_total"], summary["delta_write_success"],
                         summary["delta_write_errors"], summary["delta_events_total"],
                         summary["delta_events_published"], summary["delta_events_failed"]])

    print(f"  Requests={total_requests}  Success={successful}  Errors={errors}")
    print(f"  p50={summary['p50_ms']}ms  p95={summary['p95_ms']}ms  p99={summary['p99_ms']}ms")
    print(f"  Mean={summary['mean_ms']}ms  Elapsed={summary['elapsed_s']}s  Throughput={summary['throughput_req_s']} req/s")
    print(f"  Output: {req_csv}, {sum_csv}")
    return summary, records


# ---------------------------------------------------------------------------
# Real Replica Verification (deterministic sampled-document verification)
# ---------------------------------------------------------------------------

def verify_replicas(doc_ids_sample: List[int]) -> dict:
    """
    Deterministic sampled-document verification.
    Verifies that each document ID in doc_ids_sample is present and retrievable
    on Node 0, Node 1, and Node 2 using the read API (/node/get on each node's RPC port).
    """
    nodes = [
        ("Node 0", 9081),
        ("Node 1", 9082),
        ("Node 2", 9083),
    ]

    results = {
        "sample_size": len(doc_ids_sample),
        "sample_ids": doc_ids_sample,
        "node_results": {},
        "missing_per_node": {},
        "all_verified": True,
    }

    if not doc_ids_sample:
        results["all_verified"] = False
        for node_name, _ in nodes:
            results["node_results"][node_name] = "0/0 verified (empty sample)"
            results["missing_per_node"][node_name] = []
        return results

    for node_name, rpc_port in nodes:
        verified_count = 0
        missing_ids = []
        for doc_id in doc_ids_sample:
            found = False
            # Route to shard: std::hash<doc_id>{}(id) % 3.
            # Shard ids are 0, 1, 2. Try doc_id % 3 first, then any other shard.
            candidate_shards = [doc_id % 3] + [s for s in range(3) if s != doc_id % 3]
            for sid in candidate_shards:
                try:
                    conn = http.client.HTTPConnection(HOST, rpc_port, timeout=5.0)
                    body = json.dumps({"shard_id": sid, "document_id": doc_id})
                    conn.request("POST", "/node/get", body=body, headers={"Content-Type": "application/json"})
                    resp = conn.getresponse()
                    raw = resp.read().decode("utf-8", errors="replace")
                    conn.close()
                    if resp.status == 200:
                        data = json.loads(raw)
                        if data.get("found", False) is True:
                            found = True
                            break
                except Exception:
                    pass

            if found:
                verified_count += 1
            else:
                missing_ids.append(doc_id)

        results["missing_per_node"][node_name] = missing_ids
        if missing_ids:
            results["all_verified"] = False
            results["node_results"][node_name] = (
                f"{verified_count}/{len(doc_ids_sample)} verified (FAILED - missing IDs: {missing_ids})"
            )
        else:
            results["node_results"][node_name] = f"{verified_count}/{len(doc_ids_sample)} verified (PASS)"

    return results


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--concurrency", type=int, required=True)
    parser.add_argument("--run-id", type=str, required=True)
    parser.add_argument("--out-prefix", type=str, required=True)
    parser.add_argument("--warmup", type=int, default=100)
    parser.add_argument("--measured", type=int, default=500)
    parser.add_argument("--sample-size", type=int, default=10)
    args = parser.parse_args()

    print(f"Phase 21 Benchmark C — 3-Node Indexing")
    print(f"  Concurrency: {args.concurrency}")
    print(f"  Run ID: {args.run_id}")

    # Health check all nodes
    for port in [8081, 8082, 8083]:
        if not health_check(port):
            print(f"ERROR: Node on port {port} is not healthy.", file=sys.stderr)
            sys.exit(1)
    print("  All 3 nodes healthy.")

    # 1. Measured run (100 warmup + 500 measured POST requests)
    summary, records = run_benchmark(
        concurrency=args.concurrency,
        warmup_count=args.warmup,
        measured_count=args.measured,
        run_id=args.run_id,
        out_prefix=args.out_prefix,
    )

    if summary["errors"] > 0 or summary["successful"] != args.measured:
        print(f"ERROR: indexing workload failed: expected {args.measured} successes, got {summary['successful']}", file=sys.stderr)
        sys.exit(2)

    # 2. Select deterministic sample of successful measured document IDs
    successful_ids = [r.doc_id for r in records if r.success]
    sample_size = min(args.sample_size, len(successful_ids))
    if sample_size > 0:
        step = max(1, len(successful_ids) // sample_size)
        doc_ids_sample = [successful_ids[i * step] for i in range(sample_size)]
    else:
        doc_ids_sample = []

    print("\n--- REPLICA VERIFICATION ---")
    print(f"sample size: {len(doc_ids_sample)}")
    print(f"sample IDs: {doc_ids_sample}")

    # 3. Actually invoke verify_replicas() during Benchmark C while all 3 nodes are still running
    verification = verify_replicas(doc_ids_sample)
    print(f"Node 0 verification result: {verification['node_results'].get('Node 0', 'N/A')}")
    print(f"Node 1 verification result: {verification['node_results'].get('Node 1', 'N/A')}")
    print(f"Node 2 verification result: {verification['node_results'].get('Node 2', 'N/A')}")

    # 4. If any sampled document is missing from any node, fail the benchmark and report details
    if not verification["all_verified"]:
        print(f"\nERROR: Replica verification FAILED! Missing documents detected on replicas:", file=sys.stderr)
        for node_name, missing_list in verification["missing_per_node"].items():
            if missing_list:
                print(f"  {node_name}: missing IDs {missing_list}", file=sys.stderr)
        sys.exit(3)

    print("\nReplica verification: PASS (all sampled IDs present on Node 0, Node 1, and Node 2)")
    sys.exit(0)


if __name__ == "__main__":
    main()
