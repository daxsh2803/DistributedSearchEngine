#!/usr/bin/env python3
"""
Phase 21 Benchmark E - 3-Node Mixed Workload Benchmark Harness.

Measures mixed read/write performance on a real 3-node distributed topology:
  - Node 0: HTTP 8081, RPC 9081 (coordinator / benchmark target)
  - Node 1: HTTP 8082, RPC 9082 (replica peer)
  - Node 2: HTTP 8083, RPC 9083 (replica peer)
  - 3 shards, replication factor 3, synchronous replication (Phase 17).
  - Kafka = OFF.

Workload:
  - Pre-populated initial dataset: 510 documents (500 Moderate payload + 10 coverage documents).
  - Mixed workload ratio: 70% writes (POST /documents) / 30% searches (GET /search).
  - Concurrency levels: C1, C2, C4, C8, C16.
  - Warmup: 50 mixed requests (35 writes, 15 searches, discarded).
  - Measured: 500 mixed requests (350 writes, 150 searches) per concurrency level.
  - Concurrency overlap: Tasks partitioned round-robin across worker threads with
    persistent HTTP keep-alive connections. For concurrency > 1, reads and writes
    execute concurrently.
  - Document ID generation: Non-overlapping, unique document ID ranges per concurrency
    level (starting at 2,000,000+), avoiding any collisions with initial dataset or
    previous benchmarks.
  - Search terms: Deterministic cycling over the approved 10-term QUERY_TERMS set.

Correctness:
  - Writes: HTTP 201, valid JSON, document_id match, terms_indexed > 0.
  - Searches: HTTP 200, valid JSON, total > 0, complete == True, results is list.
  - Post-run replica verification: Sampled successfully written document IDs verified
    via /node/get on Node 0, Node 1, and Node 2.

Metrics:
  - Overall: total, success, errors, p50, p95, p99, mean, elapsed, throughput.
  - Write breakdown: total, success, errors, p50, p95, p99, mean.
  - Search breakdown: total, success, errors, p50, p95, p99, mean.
  - Server-side deltas: coordinator writes/searches, node writes/searches, EventStore stats.
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

# ---------------------------------------------------------------------------
# Query Terms & Content Payloads
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

# Distinct document ID ranges per concurrency level (starting at 2,000,000+)
# guarantees no collision with initial dataset (500k, 1000k) or previous runs.
CONCURRENCY_WRITE_CONFIG = {
    1:  {"warmup_start": 2_010_001, "measured_start": 2_011_001},
    2:  {"warmup_start": 2_020_001, "measured_start": 2_021_001},
    4:  {"warmup_start": 2_040_001, "measured_start": 2_041_001},
    8:  {"warmup_start": 2_080_001, "measured_start": 2_081_001},
    16: {"warmup_start": 2_160_001, "measured_start": 2_161_001},
}


# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------

@dataclass
class MixedTask:
    request_id: int
    op_type: str        # "write" or "search"
    target: str         # doc_id str for write, query_term for search
    doc_id: int         # 0 for search
    query_term: str     # "" for write


@dataclass
class MixedRecord:
    run_id: str
    concurrency: int
    request_id: int
    operation: str      # "write" or "search"
    target: str
    method: str         # "POST" or "GET"
    status_code: int
    success: bool
    latency_ms: float
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
            body_res = resp.read().decode("utf-8", errors="replace")
            return resp.status, body_res
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
# Response Validation
# ---------------------------------------------------------------------------

def validate_write_response(status: int, body: str, expected_doc_id: int) -> Tuple[bool, str]:
    """Validates HTTP write response for POST /documents.

    Requires HTTP 201, valid JSON, matching document_id, and terms_indexed > 0.
    """
    if status != 201:
        return False, f"HTTP {status}"
    try:
        data = json.loads(body)
        doc_id = data.get("document_id")
        terms_indexed = data.get("terms_indexed")
        if doc_id != expected_doc_id:
            return False, f"document_id mismatch: expected {expected_doc_id}, got {doc_id}"
        if not isinstance(terms_indexed, int) or terms_indexed <= 0:
            return False, f"terms_indexed={terms_indexed} (not > 0)"
        return True, ""
    except Exception as exc:
        return False, f"JSON parse error: {exc}"


def validate_search_response(status: int, body: str) -> Tuple[bool, int, bool, str]:
    """Validates HTTP search response for GET /search.

    Requires HTTP 200, valid JSON, total > 0, complete == True, results is list.
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
# Dataset Seeding & Initial Verification
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
# Workload Task Generation (Deterministic 70/30 Mixed Workload)
# ---------------------------------------------------------------------------

def build_mixed_tasks(count: int, write_start_id: int) -> List[MixedTask]:
    """Builds deterministic mixed task list with exact 70% writes / 30% searches.

    Cycle of 10 requests:
      positions 0..6 (7 tasks, 70%): WRITE (POST /documents) with unique doc_id
      positions 7..9 (3 tasks, 30%): SEARCH (GET /search) cycling QUERY_TERMS
    """
    tasks: List[MixedTask] = []
    write_offset = 0
    search_offset = 0

    for i in range(count):
        pos = i % 10
        if pos < 7:
            # Write task
            doc_id = write_start_id + write_offset
            write_offset += 1
            tasks.append(MixedTask(
                request_id=i,
                op_type="write",
                target=str(doc_id),
                doc_id=doc_id,
                query_term="",
            ))
        else:
            # Search task
            term = QUERY_TERMS[search_offset % len(QUERY_TERMS)]
            search_offset += 1
            tasks.append(MixedTask(
                request_id=i,
                op_type="search",
                target=term,
                doc_id=0,
                query_term=term,
            ))

    return tasks


# ---------------------------------------------------------------------------
# Worker Thread Execution
# ---------------------------------------------------------------------------

def _mixed_worker(
    task_queue: List[MixedTask],
    records_out: List[MixedRecord],
    records_lock: threading.Lock,
    is_warmup: bool,
    run_id: str,
    concurrency: int,
    host: str,
    port: int,
) -> None:
    conn = PersistentConn(host, port)
    try:
        for task in task_queue:
            status = 0
            err_str = ""
            success = False

            t0 = time.perf_counter()
            if task.op_type == "write":
                method = "POST"
                path = "/documents"
                body = json.dumps({"id": task.doc_id, "content": CONTENT_M})
                try:
                    status, resp_body = conn.post(path, body)
                    success, err_str = validate_write_response(status, resp_body, task.doc_id)
                except Exception as exc:
                    err_str = str(exc)
                    success = False
            else:
                method = "GET"
                path = f"/search?q={urllib.parse.quote(task.query_term)}&mode=or"
                try:
                    status, resp_body = conn.get(path)
                    success, _, _, err_str = validate_search_response(status, resp_body)
                except Exception as exc:
                    err_str = str(exc)
                    success = False

            t1 = time.perf_counter()
            latency_ms = (t1 - t0) * 1000.0

            if not is_warmup:
                rec = MixedRecord(
                    run_id=run_id,
                    concurrency=concurrency,
                    request_id=task.request_id,
                    operation=task.op_type,
                    target=task.target,
                    method=method,
                    status_code=status,
                    success=success,
                    latency_ms=round(latency_ms, 4),
                    error=err_str,
                )
                with records_lock:
                    records_out.append(rec)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Real Replica Verification (Sampled-Document RPC Inspection)
# ---------------------------------------------------------------------------

def verify_replicas(doc_ids_sample: List[int]) -> dict:
    """Verifies that each document ID in doc_ids_sample is present and retrievable

    on Node 0, Node 1, and Node 2 using /node/get on each node's RPC port.
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
            # Route to shard: try doc_id % 3 first, then any other shard.
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
) -> Tuple[dict, List[MixedRecord]]:
    print(f"\n================================================================")
    print(f"Benchmark E - Concurrency={concurrency} (Run ID: {run_id})")
    print(f"Target: http://{host}:{port}")
    print(f"Workload: 70% writes (POST /documents) / 30% searches (GET /search)")
    print(f"================================================================")

    # 1. Validate dataset is ready
    if not validate_queries(port=port, host=host):
        raise RuntimeError("Dataset validation failed: not all query terms return valid results.")

    # 2. Get ID ranges for this concurrency level
    cfg = CONCURRENCY_WRITE_CONFIG.get(concurrency, {
        "warmup_start": 2_000_000 + concurrency * 10_000 + 1,
        "measured_start": 2_000_000 + concurrency * 10_000 + 1001,
    })

    # 3. Warmup tasks (round-robin distributed across worker threads)
    warmup_tasks = build_mixed_tasks(warmup_count, cfg["warmup_start"])
    warmup_chunks = [[] for _ in range(concurrency)]
    for i, t in enumerate(warmup_tasks):
        warmup_chunks[i % concurrency].append(t)

    print(f"  Running {warmup_count} warmup mixed requests across {concurrency} workers (discarded)...")
    warmup_records: List[MixedRecord] = []
    warmup_lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [
            ex.submit(
                _mixed_worker,
                chunk, warmup_records, warmup_lock, True,
                run_id, concurrency, host, port
            )
            for chunk in warmup_chunks if chunk
        ]
        for f in as_completed(futs):
            f.result()
    print(f"  Warmup complete ({warmup_count} requests discarded).")

    # 4. Snapshot metrics BEFORE measured requests
    metrics_before = get_metrics(port=port, host=host)
    coord_writes_before = metrics_before.get("coordinator_writes_total", 0)
    coord_write_success_before = metrics_before.get("coordinator_write_success", 0)
    coord_write_errors_before = metrics_before.get("coordinator_write_errors", 0)
    coord_searches_before = metrics_before.get("coordinator_searches_total", 0)
    coord_search_success_before = metrics_before.get("coordinator_search_success", 0)
    coord_search_errors_before = metrics_before.get("coordinator_search_errors", 0)
    writes_total_before = metrics_before.get("writes_total", 0)
    write_errors_before = metrics_before.get("write_errors", 0)
    searches_total_before = metrics_before.get("searches_total", 0)
    search_errors_before = metrics_before.get("search_errors", 0)
    events_total_before = metrics_before.get("events_total", 0)
    events_pub_before = metrics_before.get("events_published", 0)
    events_failed_before = metrics_before.get("events_failed", 0)

    print(f"  Metrics BEFORE:")
    print(f"    coordinator_writes_total={coord_writes_before}  coordinator_searches_total={coord_searches_before}")
    print(f"    writes_total={writes_total_before}  searches_total={searches_total_before}")
    print(f"    events_total={events_total_before}  events_published={events_pub_before}")

    # 5. Measured run: round-robin task partition for genuine concurrent read/write overlap
    meas_tasks = build_mixed_tasks(measured_count, cfg["measured_start"])
    meas_chunks = [[] for _ in range(concurrency)]
    for i, t in enumerate(meas_tasks):
        meas_chunks[i % concurrency].append(t)

    expected_writes = sum(1 for t in meas_tasks if t.op_type == "write")
    expected_searches = sum(1 for t in meas_tasks if t.op_type == "search")
    print(f"  Running {measured_count} measured mixed requests ({expected_writes} writes, {expected_searches} searches) at concurrency={concurrency}...")

    records: List[MixedRecord] = []
    records_lock = threading.Lock()

    t_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [
            ex.submit(
                _mixed_worker,
                chunk, records, records_lock, False,
                run_id, concurrency, host, port
            )
            for chunk in meas_chunks if chunk
        ]
        for f in as_completed(futs):
            f.result()
    t_end = time.perf_counter()
    elapsed_s = t_end - t_start

    # 6. Snapshot metrics AFTER measured requests
    metrics_after = get_metrics(port=port, host=host)
    coord_writes_after = metrics_after.get("coordinator_writes_total", 0)
    coord_write_success_after = metrics_after.get("coordinator_write_success", 0)
    coord_write_errors_after = metrics_after.get("coordinator_write_errors", 0)
    coord_searches_after = metrics_after.get("coordinator_searches_total", 0)
    coord_search_success_after = metrics_after.get("coordinator_search_success", 0)
    coord_search_errors_after = metrics_after.get("coordinator_search_errors", 0)
    writes_total_after = metrics_after.get("writes_total", 0)
    write_errors_after = metrics_after.get("write_errors", 0)
    searches_total_after = metrics_after.get("searches_total", 0)
    search_errors_after = metrics_after.get("search_errors", 0)
    events_total_after = metrics_after.get("events_total", 0)
    events_pub_after = metrics_after.get("events_published", 0)
    events_failed_after = metrics_after.get("events_failed", 0)

    delta_coord_writes = coord_writes_after - coord_writes_before
    delta_coord_write_success = coord_write_success_after - coord_write_success_before
    delta_coord_write_errors = coord_write_errors_after - coord_write_errors_before
    delta_coord_searches = coord_searches_after - coord_searches_before
    delta_coord_search_success = coord_search_success_after - coord_search_success_before
    delta_coord_search_errors = coord_search_errors_after - coord_search_errors_before
    delta_writes_total = writes_total_after - writes_total_before
    delta_write_errors = write_errors_after - write_errors_before
    delta_searches_total = searches_total_after - searches_total_before
    delta_search_errors = search_errors_after - search_errors_before
    delta_events_total = events_total_after - events_total_before
    delta_events_published = events_pub_after - events_pub_before
    delta_events_failed = events_failed_after - events_failed_before

    print(f"  Metrics AFTER:")
    print(f"    coordinator_writes_total={coord_writes_after}  coordinator_searches_total={coord_searches_after}")
    print(f"  Deltas:")
    print(f"    delta_coordinator_writes={delta_coord_writes} (success={delta_coord_write_success}, err={delta_coord_write_errors})")
    print(f"    delta_coordinator_searches={delta_coord_searches} (success={delta_coord_search_success}, err={delta_coord_search_errors})")
    print(f"    delta_writes_total={delta_writes_total}  delta_searches_total={delta_searches_total}")
    print(f"    delta_events_published={delta_events_published}  delta_events_failed={delta_events_failed}")

    # 7. Compute statistics (Overall, Writes, Searches)
    records.sort(key=lambda r: r.request_id)
    total_requests = len(records)
    successful = sum(1 for r in records if r.success)
    errors = total_requests - successful
    all_latencies = [r.latency_ms for r in records if r.success]
    p50, p95, p99, mean_ms = compute_percentiles(all_latencies)
    throughput = total_requests / elapsed_s if elapsed_s > 0 else 0.0

    write_records = [r for r in records if r.operation == "write"]
    writes_count = len(write_records)
    writes_successful = sum(1 for r in write_records if r.success)
    writes_errors = writes_count - writes_successful
    write_latencies = [r.latency_ms for r in write_records if r.success]
    w_p50, w_p95, w_p99, w_mean = compute_percentiles(write_latencies)

    search_records = [r for r in records if r.operation == "search"]
    searches_count = len(search_records)
    searches_successful = sum(1 for r in search_records if r.success)
    searches_errors = searches_count - searches_successful
    search_latencies = [r.latency_ms for r in search_records if r.success]
    s_p50, s_p95, s_p99, s_mean = compute_percentiles(search_latencies)

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
        # Writes breakdown
        "writes_total": writes_count,
        "writes_successful": writes_successful,
        "writes_errors": writes_errors,
        "write_p50_ms": round(w_p50, 4),
        "write_p95_ms": round(w_p95, 4),
        "write_p99_ms": round(w_p99, 4),
        "write_mean_ms": round(w_mean, 4),
        # Searches breakdown
        "searches_total": searches_count,
        "searches_successful": searches_successful,
        "searches_errors": searches_errors,
        "search_p50_ms": round(s_p50, 4),
        "search_p95_ms": round(s_p95, 4),
        "search_p99_ms": round(s_p99, 4),
        "search_mean_ms": round(s_mean, 4),
        # Server-side deltas
        "delta_coordinator_writes": delta_coord_writes,
        "delta_coordinator_write_success": delta_coord_write_success,
        "delta_coordinator_write_errors": delta_coord_write_errors,
        "delta_coordinator_searches": delta_coord_searches,
        "delta_coordinator_search_success": delta_coord_search_success,
        "delta_coordinator_search_errors": delta_coord_search_errors,
        "delta_writes_total": delta_writes_total,
        "delta_write_errors": delta_write_errors,
        "delta_searches_total": delta_searches_total,
        "delta_search_errors": delta_search_errors,
        "delta_events_total": delta_events_total,
        "delta_events_published": delta_events_published,
        "delta_events_failed": delta_events_failed,
    }

    # 8. Write per-request CSV
    out_dir = os.path.dirname(out_prefix)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    req_csv = f"{out_prefix}_requests.csv"
    with open(req_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "run_id", "concurrency", "request_id", "operation",
            "target", "method", "status_code", "success", "latency_ms", "error"
        ])
        for r in records:
            writer.writerow([
                r.run_id, r.concurrency, r.request_id, r.operation,
                r.target, r.method, r.status_code, r.success, r.latency_ms, r.error
            ])

    # 9. Write per-concurrency summary CSV
    sum_csv = f"{out_prefix}_summary.csv"
    with open(sum_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(list(summary.keys()))
        writer.writerow(list(summary.values()))

    print(f"\n  OVERALL: Requests={total_requests}  Success={successful}  Errors={errors}")
    print(f"    p50={summary['p50_ms']}ms  p95={summary['p95_ms']}ms  p99={summary['p99_ms']}ms  Mean={summary['mean_ms']}ms")
    print(f"    Elapsed={summary['elapsed_s']}s  Throughput={summary['throughput_req_s']} req/s")
    print(f"  WRITES (70%): Count={writes_count}  Success={writes_successful}  Errors={writes_errors}")
    print(f"    p50={summary['write_p50_ms']}ms  p95={summary['write_p95_ms']}ms  p99={summary['write_p99_ms']}ms  Mean={summary['write_mean_ms']}ms")
    print(f"  SEARCHES (30%): Count={searches_count}  Success={searches_successful}  Errors={searches_errors}")
    print(f"    p50={summary['search_p50_ms']}ms  p95={summary['search_p95_ms']}ms  p99={summary['search_p99_ms']}ms  Mean={summary['search_mean_ms']}ms")
    print(f"  Written: {req_csv}")
    print(f"  Written: {sum_csv}")

    return summary, records


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 21 Benchmark E - 3-Node Mixed Workload Harness")
    parser.add_argument("--concurrency", type=int, help="Worker concurrency level (1, 2, 4, 8, 16)")
    parser.add_argument("--run-id", type=str, help="Run identifier (e.g. E_c1)")
    parser.add_argument("--out-prefix", type=str, help="Output file prefix (e.g. results/phase21_E_mixed_3node_c1)")
    parser.add_argument("--warmup", type=int, default=50, help="Number of warmup requests (default: 50)")
    parser.add_argument("--measured", type=int, default=500, help="Number of measured requests (default: 500)")
    parser.add_argument("--sample-size", type=int, default=10, help="Sample size for replica verification (default: 10)")
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

    summary, records = run_benchmark(
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

    # Replica verification on sampled successfully written document IDs
    successful_write_ids = [int(r.target) for r in records if r.operation == "write" and r.success]
    sample_size = min(args.sample_size, len(successful_write_ids))
    if sample_size > 0:
        step = max(1, len(successful_write_ids) // sample_size)
        doc_ids_sample = [successful_write_ids[i * step] for i in range(sample_size)]
    else:
        doc_ids_sample = []

    print("\n--- REPLICA VERIFICATION ---")
    print(f"Sample size: {len(doc_ids_sample)}")
    print(f"Sample IDs: {doc_ids_sample}")

    verification = verify_replicas(doc_ids_sample)
    print(f"Node 0 verification: {verification['node_results'].get('Node 0', 'N/A')}")
    print(f"Node 1 verification: {verification['node_results'].get('Node 1', 'N/A')}")
    print(f"Node 2 verification: {verification['node_results'].get('Node 2', 'N/A')}")

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
