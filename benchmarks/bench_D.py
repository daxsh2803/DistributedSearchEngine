#!/usr/bin/env python3
"""
Phase 21 Benchmark D — 3-Node Search Benchmark Harness.

Measures HTTP search performance on a real 3-node distributed topology:
  - Node 0: HTTP 8081, RPC 9081 (coordinator/search target)
  - Node 1: HTTP 8082, RPC 9082 (replica peer)
  - Node 2: HTTP 8083, RPC 9083 (replica peer)
  - 3 shards, replication factor 3, synchronous replication (Phase 17).

Workload:
  - Pre-populated dataset: 510 documents (500 Moderate payload + 10 coverage documents).
  - 50 warmup searches (discarded).
  - 500 measured searches per concurrency level (C1, C2, C4, C8, C16).
  - 10 deterministic query terms: fox, quick, search, document, distributed,
    index, kafka, node, shard, engine.
  - Mode: OR.

Correctness:
  - HTTP 200, valid JSON.
  - 'total' is integer and > 0.
  - 'complete' is True (all shards responded).
  - 'results' is a list.

Percentiles:
  - statistics.quantiles(latencies, n=100, method="inclusive")
"""

import argparse
import csv
import http.client
import json
import os
import pathlib
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

# ---------------------------------------------------------------------------
# Query Terms & Dataset Constants
# ---------------------------------------------------------------------------

QUERY_TERMS = [
    "fox", "quick", "search", "document", "distributed",
    "index", "kafka", "node", "shard", "engine",
]

CONTENT_M = (
    "distributed search engines index large document collections using "
    "inverted index structures with tf-idf scoring algorithms for ranked "
    "retrieval supporting boolean query modes including or and and with "
    "concurrent multi-shard fan-out across replicated node clusters"
)

# 10 extra coverage documents ensuring every query term in QUERY_TERMS
# returns total > 0 (covering fox, quick, kafka, shard, engine).
EXTRA_DOCS: Dict[int, str] = {
    1_000_001: "the quick brown fox jumps over the lazy dog fox fox",
    1_000_002: "kafka message broker event streaming distributed kafka kafka",
    1_000_003: "shard routing inverted index shard placement shard shard",
    1_000_004: "search engine full text engine ranked results engine engine",
    1_000_005: "fox quick brown quick fox quick quick kafka shard engine",
    1_000_006: "kafka streams shard coordinator node engine kafka engine shard",
    1_000_007: "quick fox lazy dog engine kafka shard quick fox engine",
    1_000_008: "distributed engine kafka shard quick fox index engine shard",
    1_000_009: "search engine node shard kafka quick fox engine shard node",
    1_000_010: "kafka engine fox quick shard distributed search index node engine",
}


# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------

@dataclass
class SearchRecord:
    run_id: str
    concurrency: int
    request_id: int
    query_term: str
    status_code: int
    success: bool
    latency_ms: float
    total_results: int
    complete: bool
    error: str = ""


# ---------------------------------------------------------------------------
# Persistent HTTP Connection Worker
# ---------------------------------------------------------------------------

class PersistentConn:
    def __init__(self, host: str, port: int, timeout: float = 30.0):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._conn: Optional[http.client.HTTPConnection] = None
        self._connect()

    def _connect(self) -> None:
        try:
            self._conn = http.client.HTTPConnection(
                self._host, self._port, timeout=self._timeout
            )
            self._conn.connect()
        except Exception:
            self._conn = None

    def get(self, path: str) -> Tuple[int, str]:
        try:
            if self._conn is None:
                self._connect()
                if self._conn is None:
                    raise RuntimeError("Could not establish connection to server")
            self._conn.request("GET", path)
            resp = self._conn.getresponse()
            body = resp.read().decode("utf-8", errors="replace")
            return resp.status, body
        except Exception as exc:
            self.close()
            raise RuntimeError(str(exc)) from exc

    def post(self, path: str, body: str) -> Tuple[int, str]:
        encoded = body.encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(encoded)),
        }
        try:
            if self._conn is None:
                self._connect()
                if self._conn is None:
                    raise RuntimeError("Could not establish connection to server")
            self._conn.request("POST", path, body=body, headers=headers)
            resp = self._conn.getresponse()
            body = resp.read().decode("utf-8", errors="replace")
            return resp.status, body
        except Exception as exc:
            self.close()
            raise RuntimeError(str(exc)) from exc

    def close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None


# ---------------------------------------------------------------------------
# Health & Metrics Helpers
# ---------------------------------------------------------------------------

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


def wait_healthy(port: int, host: str = HOST, timeout_s: float = 30.0) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if health_check(port, host):
            return True
        time.sleep(0.2)
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


# ---------------------------------------------------------------------------
# Percentiles (Approved Phase 21 Methodology)
# ---------------------------------------------------------------------------

def compute_percentiles(samples: List[float]) -> Tuple[float, float, float, float]:
    """Return (p50, p95, p99, mean) in milliseconds.

    Methodology: statistics.quantiles(data, n=100, method="inclusive").
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
        s = sorted(samples)
        p50 = s[max(0, int(0.50 * n) - 1)]
        p95 = s[max(0, int(0.95 * n) - 1)]
        p99 = s[max(0, int(0.99 * n) - 1)]
        return p50, p95, p99, mean


# ---------------------------------------------------------------------------
# Search Response Validation
# ---------------------------------------------------------------------------

def validate_search_response(status: int, body: str) -> Tuple[bool, int, bool, str]:
    """Validates HTTP search response.

    Returns (is_valid, total_count, is_complete, error_message).
    """
    if status != 200:
        return False, 0, False, f"HTTP {status}"
    try:
        data = json.loads(body)
        total = data.get("total")
        complete = data.get("complete", False)
        results = data.get("results")

        if not isinstance(total, int) or total <= 0:
            return False, total if isinstance(total, int) else 0, bool(complete), f"total={total} (not > 0)"
        if not complete:
            return False, total, False, "incomplete search (partial shard failure)"
        if not isinstance(results, list):
            return False, total, True, "results field is not a list"

        return True, total, True, ""
    except Exception as exc:
        return False, 0, False, f"JSON parse error: {exc}"


# ---------------------------------------------------------------------------
# Dataset Seeding & Verification
# ---------------------------------------------------------------------------

def seed_dataset(port: int = DEFAULT_TARGET_PORT, host: str = HOST) -> bool:
    """Seeds 500 Moderate documents + 10 coverage documents via Node 0 HTTP.

    With replica-factor=3, Node 0 replicates all documents to all 3 nodes.
    """
    print(f"\n[Dataset Preparation] Connecting to Node 0 on port {port}...")
    conn = PersistentConn(host, port)
    seeded_count = 0
    errors = 0

    try:
        # 1. 500 Moderate payload documents (IDs 500001 - 500500)
        print("  Seeding 500 Moderate documents (IDs 500001 - 500500)...")
        for i in range(500):
            doc_id = 500_001 + i
            body = json.dumps({"id": doc_id, "content": CONTENT_M})
            try:
                status, resp_body = conn.post("/documents", body)
                if status in (201, 409):
                    seeded_count += 1
                else:
                    errors += 1
                    print(f"    Error seeding doc_id={doc_id}: HTTP {status} {resp_body[:60]}", file=sys.stderr)
            except Exception as exc:
                errors += 1
                print(f"    Exception seeding doc_id={doc_id}: {exc}", file=sys.stderr)

        # 2. 10 extra coverage documents (IDs 1000001 - 1000010)
        print("  Seeding 10 extra coverage documents (IDs 1000001 - 1000010)...")
        for doc_id, content in EXTRA_DOCS.items():
            body = json.dumps({"id": doc_id, "content": content})
            try:
                status, resp_body = conn.post("/documents", body)
                if status in (201, 409):
                    seeded_count += 1
                else:
                    errors += 1
                    print(f"    Error seeding coverage doc_id={doc_id}: HTTP {status} {resp_body[:60]}", file=sys.stderr)
            except Exception as exc:
                errors += 1
                print(f"    Exception seeding coverage doc_id={doc_id}: {exc}", file=sys.stderr)
    finally:
        conn.close()

    total_target = 500 + len(EXTRA_DOCS)
    print(f"  Dataset seeding completed: {seeded_count}/{total_target} OK (errors: {errors})")
    return errors == 0 and seeded_count == total_target


def validate_queries(port: int = DEFAULT_TARGET_PORT, host: str = HOST) -> bool:
    """Verifies that all 10 QUERY_TERMS return complete=True and total > 0."""
    print("\n[Dataset Validation] Testing all 10 QUERY_TERMS...")
    conn = PersistentConn(host, port)
    all_ok = True

    try:
        for term in QUERY_TERMS:
            path = f"/search?q={urllib.parse.quote(term)}&mode=or"
            try:
                status, body = conn.get(path)
                is_valid, total, complete, err = validate_search_response(status, body)
                if is_valid:
                    print(f"    [PASS] '{term}' -> total={total}, complete={complete}")
                else:
                    print(f"    [FAIL] '{term}' -> {err} (HTTP {status})", file=sys.stderr)
                    all_ok = False
            except Exception as exc:
                print(f"    [FAIL] '{term}' -> exception: {exc}", file=sys.stderr)
                all_ok = False
    finally:
        conn.close()

    if all_ok:
        print("  All 10 QUERY_TERMS validated successfully.")
    else:
        print("  Dataset validation FAILED.", file=sys.stderr)
    return all_ok


# ---------------------------------------------------------------------------
# Worker Thread Execution
# ---------------------------------------------------------------------------

def _search_worker(
    task_queue: List[Tuple[int, str]],
    records_out: List[SearchRecord],
    records_lock: threading.Lock,
    is_warmup: bool,
    run_id: str,
    concurrency: int,
    host: str,
    port: int,
) -> None:
    conn = PersistentConn(host, port)
    try:
        for req_id, query_term in task_queue:
            path = f"/search?q={urllib.parse.quote(query_term)}&mode=or"
            status = 0
            err_str = ""
            total_results = 0
            complete = False
            success = False

            t0 = time.perf_counter()
            try:
                status, body = conn.get(path)
                success, total_results, complete, err_str = validate_search_response(status, body)
            except Exception as exc:
                err_str = str(exc)
                success = False
            t1 = time.perf_counter()
            latency_ms = (t1 - t0) * 1000.0

            if not is_warmup:
                rec = SearchRecord(
                    run_id=run_id,
                    concurrency=concurrency,
                    request_id=req_id,
                    query_term=query_term,
                    status_code=status,
                    success=success,
                    latency_ms=round(latency_ms, 4),
                    total_results=total_results,
                    complete=complete,
                    error=err_str,
                )
                with records_lock:
                    records_out.append(rec)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Benchmark Runner (Single Concurrency Level)
# ---------------------------------------------------------------------------

def run_benchmark(
    concurrency: int,
    warmup_count: int,
    measured_count: int,
    run_id: str,
    out_prefix: str,
    port: int = DEFAULT_TARGET_PORT,
    host: str = HOST,
) -> Tuple[dict, List[SearchRecord]]:
    print(f"\n================================================================")
    print(f"Benchmark D - Concurrency={concurrency} (Run ID: {run_id})")
    print(f"Target: http://{host}:{port}/search")
    print(f"================================================================")

    # 1. Validate dataset is ready
    if not validate_queries(port=port, host=host):
        raise RuntimeError("Dataset validation failed: not all query terms return valid results.")

    # 2. Warmup tasks (deterministic cycling over query terms)
    warmup_tasks = [(i, QUERY_TERMS[i % len(QUERY_TERMS)]) for i in range(warmup_count)]
    chunk_size = max(1, len(warmup_tasks) // concurrency)
    warmup_chunks = [warmup_tasks[i:i + chunk_size] for i in range(0, len(warmup_tasks), chunk_size)]

    print(f"  Running {warmup_count} warmup searches across {concurrency} workers (discarded)...")
    warmup_records: List[SearchRecord] = []
    warmup_lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [
            ex.submit(
                _search_worker,
                chunk, warmup_records, warmup_lock, True,
                run_id, concurrency, host, port
            )
            for chunk in warmup_chunks
        ]
        for f in as_completed(futs):
            f.result()
    print(f"  Warmup complete ({warmup_count} requests discarded).")

    # 3. Snapshot metrics BEFORE measured requests
    metrics_before = get_metrics(port=port, host=host)
    coord_searches_before = metrics_before.get("coordinator_searches_total", 0)
    coord_success_before = metrics_before.get("coordinator_search_success", 0)
    coord_errors_before = metrics_before.get("coordinator_search_errors", 0)
    searches_total_before = metrics_before.get("searches_total", 0)
    search_errors_before = metrics_before.get("search_errors", 0)

    print(f"  Metrics BEFORE:")
    print(f"    coordinator_searches_total={coord_searches_before}  coordinator_search_success={coord_success_before}  coordinator_search_errors={coord_errors_before}")
    print(f"    searches_total={searches_total_before}  search_errors={search_errors_before}")

    # 4. Measured run (500 requests)
    meas_tasks = [(i, QUERY_TERMS[i % len(QUERY_TERMS)]) for i in range(measured_count)]
    meas_chunk_size = max(1, len(meas_tasks) // concurrency)
    meas_chunks = [meas_tasks[i:i + meas_chunk_size] for i in range(0, len(meas_tasks), meas_chunk_size)]

    print(f"  Running {measured_count} measured searches at concurrency={concurrency}...")
    records: List[SearchRecord] = []
    records_lock = threading.Lock()

    t_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [
            ex.submit(
                _search_worker,
                chunk, records, records_lock, False,
                run_id, concurrency, host, port
            )
            for chunk in meas_chunks
        ]
        for f in as_completed(futs):
            f.result()
    t_end = time.perf_counter()
    elapsed_s = t_end - t_start

    # 5. Snapshot metrics AFTER measured requests
    metrics_after = get_metrics(port=port, host=host)
    coord_searches_after = metrics_after.get("coordinator_searches_total", 0)
    coord_success_after = metrics_after.get("coordinator_search_success", 0)
    coord_errors_after = metrics_after.get("coordinator_search_errors", 0)
    searches_total_after = metrics_after.get("searches_total", 0)
    search_errors_after = metrics_after.get("search_errors", 0)

    delta_coord_searches = coord_searches_after - coord_searches_before
    delta_coord_success = coord_success_after - coord_success_before
    delta_coord_errors = coord_errors_after - coord_errors_before
    delta_searches_total = searches_total_after - searches_total_before
    delta_search_errors = search_errors_after - search_errors_before

    print(f"  Metrics AFTER:")
    print(f"    coordinator_searches_total={coord_searches_after}  coordinator_search_success={coord_success_after}  coordinator_search_errors={coord_errors_after}")
    print(f"    searches_total={searches_total_after}  search_errors={search_errors_after}")
    print(f"  Deltas:")
    print(f"    delta_coordinator_searches={delta_coord_searches}  delta_coordinator_success={delta_coord_success}  delta_coordinator_errors={delta_coord_errors}")
    print(f"    delta_searches_total={delta_searches_total}  delta_search_errors={delta_search_errors}")

    # 6. Compute statistics
    records.sort(key=lambda r: r.request_id)
    total_requests = len(records)
    successful = sum(1 for r in records if r.success)
    errors = total_requests - successful
    latencies = [r.latency_ms for r in records if r.success]

    if latencies:
        p50, p95, p99, mean_ms = compute_percentiles(latencies)
    else:
        p50 = p95 = p99 = mean_ms = 0.0

    throughput = total_requests / elapsed_s if elapsed_s > 0 else 0.0

    summary = {
        "run_id": run_id,
        "concurrency": concurrency,
        "requests": total_requests,
        "successful": successful,
        "errors": errors,
        "p50_ms": round(p50, 4),
        "p95_ms": round(p95, 4),
        "p99_ms": round(p99, 4),
        "mean_ms": round(mean_ms, 4),
        "elapsed_s": round(elapsed_s, 4),
        "throughput_req_s": round(throughput, 4),
        "delta_coordinator_searches": delta_coord_searches,
        "delta_coordinator_success": delta_coord_success,
        "delta_coordinator_errors": delta_coord_errors,
        "delta_searches_total": delta_searches_total,
        "delta_search_errors": delta_search_errors,
    }

    # 7. Write per-request CSV
    out_dir = os.path.dirname(out_prefix)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    req_csv = f"{out_prefix}_requests.csv"
    with open(req_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "run_id", "concurrency", "request_id", "query_term",
            "status_code", "success", "latency_ms", "total_results",
            "complete", "error"
        ])
        for r in records:
            writer.writerow([
                r.run_id, r.concurrency, r.request_id, r.query_term,
                r.status_code, r.success, r.latency_ms, r.total_results,
                r.complete, r.error
            ])

    # 8. Write per-concurrency summary CSV
    sum_csv = f"{out_prefix}_summary.csv"
    with open(sum_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "run_id", "concurrency", "requests", "successful", "errors",
            "p50_ms", "p95_ms", "p99_ms", "mean_ms", "elapsed_s", "throughput_req_s",
            "delta_coordinator_searches", "delta_coordinator_success", "delta_coordinator_errors",
            "delta_searches_total", "delta_search_errors"
        ])
        writer.writerow([
            summary["run_id"], summary["concurrency"], summary["requests"],
            summary["successful"], summary["errors"],
            summary["p50_ms"], summary["p95_ms"], summary["p99_ms"],
            summary["mean_ms"], summary["elapsed_s"], summary["throughput_req_s"],
            summary["delta_coordinator_searches"], summary["delta_coordinator_success"],
            summary["delta_coordinator_errors"], summary["delta_searches_total"],
            summary["delta_search_errors"]
        ])

    print(f"\n  Results: Requests={total_requests}  Success={successful}  Errors={errors}")
    print(f"  p50={summary['p50_ms']}ms  p95={summary['p95_ms']}ms  p99={summary['p99_ms']}ms  Mean={summary['mean_ms']}ms")
    print(f"  Elapsed={summary['elapsed_s']}s  Throughput={summary['throughput_req_s']} req/s")
    print(f"  Written: {req_csv}")
    print(f"  Written: {sum_csv}")

    return summary, records


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 21 Benchmark D — 3-Node Search Harness")
    parser.add_argument("--concurrency", type=int, help="Worker concurrency level (1, 2, 4, 8, 16)")
    parser.add_argument("--run-id", type=str, help="Run identifier (e.g. D_c1)")
    parser.add_argument("--out-prefix", type=str, help="Output file prefix (e.g. results/phase21_D_search_3node_c1)")
    parser.add_argument("--warmup", type=int, default=50, help="Number of warmup searches (default: 50)")
    parser.add_argument("--measured", type=int, default=500, help="Number of measured searches (default: 500)")
    parser.add_argument("--target-port", type=int, default=DEFAULT_TARGET_PORT, help="Target node HTTP port (default: 8081)")
    parser.add_argument("--prepare", action="store_true", help="Seed 510 documents and validate query terms, then exit")
    args = parser.parse_args()

    # Pre-check health
    if not wait_healthy(args.target_port, timeout_s=30.0):
        print(f"ERROR: Node on port {args.target_port} is not healthy.", file=sys.stderr)
        sys.exit(1)

    if args.prepare:
        ok = seed_dataset(port=args.target_port)
        if not ok:
            print("ERROR: Dataset seeding failed.", file=sys.stderr)
            sys.exit(2)
        valid = validate_queries(port=args.target_port)
        if not valid:
            print("ERROR: Dataset validation failed.", file=sys.stderr)
            sys.exit(3)
        print("\nDataset preparation and validation complete.")
        sys.exit(0)

    if not args.concurrency or not args.run_id or not args.out_prefix:
        print("ERROR: --concurrency, --run-id, and --out-prefix are required for benchmark runs.", file=sys.stderr)
        sys.exit(1)

    summary, _ = run_benchmark(
        concurrency=args.concurrency,
        warmup_count=args.warmup,
        measured_count=args.measured,
        run_id=args.run_id,
        out_prefix=args.out_prefix,
        port=args.target_port,
    )

    if summary["errors"] > 0 or summary["successful"] != args.measured:
        print(f"ERROR: expected {args.measured} successes, got {summary['successful']}", file=sys.stderr)
        sys.exit(2)

    sys.exit(0)


if __name__ == "__main__":
    main()
