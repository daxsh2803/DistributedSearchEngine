#!/usr/bin/env python3
"""
Phase 21 Benchmark F - Replication Overhead Benchmark Harness.

Measures the performance overhead of Phase 17 synchronous replication by comparing:
  - F-RF1: 3 real nodes, replica_factor=1
  - F-RF3: 3 real nodes, replica_factor=3
  - Kafka = OFF.

Topology:
  - Node 0: HTTP 8081, RPC 9081 (coordinator / benchmark target)
  - Node 1: HTTP 8082, RPC 9082 (peer)
  - Node 2: HTTP 8083, RPC 9083 (peer)
  - peers: 0=127.0.0.1:9081,1=127.0.0.1:9082,2=127.0.0.1:9083
  - 3 shards, Kafka disabled

Workload per run:
  - Pre-populated baseline dataset: 510 documents (500 Moderate payload + 10 coverage documents).
  - Concurrency levels: C1, C2, C4, C8, C16.
  - Warmup: 100 writes (discarded).
  - Measured: 500 writes (POST /documents) with Moderate payload (CONTENT_M).
  - Unique document IDs per run (never reused).
  - Persistent HTTP keep-alive connections per worker thread.

Correctness:
  - HTTP 201 Created, valid JSON, document_id match, terms_indexed > 0.
  - Sampled replica verification (10 sampled document IDs):
      - RF=1: document must exist on Node (doc_id % 3) and must NOT exist on other nodes.
      - RF=3: document must exist on all 3 nodes (Node 0, Node 1, Node 2).

Consolidated Comparison:
  - Matches RF=1 and RF=3 at identical concurrency levels.
  - p50_overhead_percent = ((RF3 latency - RF1 latency) / RF1 latency) * 100
  - p95_overhead_percent = ((RF3 latency - RF1 latency) / RF1 latency) * 100
  - p99_overhead_percent = ((RF3 latency - RF1 latency) / RF1 latency) * 100
  - throughput_impact_percent = ((RF1 throughput - RF3 throughput) / RF1 throughput) * 100
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
# Payloads & Constants
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

# Unique document ID ranges per (replication_factor, concurrency)
# All ranges start at 3,000,000+ to ensure zero collision with baseline (500k, 1000k)
# or previous benchmarks.
WRITE_ID_CONFIG = {
    1: {  # RF = 1
        1:  {"warmup_start": 3_010_001, "measured_start": 3_011_001},
        2:  {"warmup_start": 3_020_001, "measured_start": 3_021_001},
        4:  {"warmup_start": 3_040_001, "measured_start": 3_041_001},
        8:  {"warmup_start": 3_080_001, "measured_start": 3_081_001},
        16: {"warmup_start": 3_160_001, "measured_start": 3_161_001},
    },
    3: {  # RF = 3
        1:  {"warmup_start": 3_510_001, "measured_start": 3_511_001},
        2:  {"warmup_start": 3_520_001, "measured_start": 3_521_001},
        4:  {"warmup_start": 3_540_001, "measured_start": 3_541_001},
        8:  {"warmup_start": 3_580_001, "measured_start": 3_581_001},
        16: {"warmup_start": 3_660_001, "measured_start": 3_661_001},
    },
}


# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------

@dataclass
class WriteRecord:
    run_id: str
    replication_factor: int
    concurrency: int
    request_id: int
    doc_id: int
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
    """Validates HTTP search response for GET /search."""
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
    """Seeds 500 Moderate documents + 10 coverage documents via Node 0 HTTP."""
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

def _write_worker(
    task_queue: List[Tuple[int, int]],
    records_out: List[WriteRecord],
    records_lock: threading.Lock,
    is_warmup: bool,
    run_id: str,
    replication_factor: int,
    concurrency: int,
    host: str,
    port: int,
) -> None:
    conn = PersistentConn(host, port)
    try:
        for req_id, doc_id in task_queue:
            status = 0
            err_str = ""
            success = False

            body = json.dumps({"id": doc_id, "content": CONTENT_M})
            t0 = time.perf_counter()
            try:
                status, resp_body = conn.post("/documents", body)
                success, err_str = validate_write_response(status, resp_body, doc_id)
            except Exception as exc:
                err_str = str(exc)
                success = False

            t1 = time.perf_counter()
            latency_ms = (t1 - t0) * 1000.0

            if not is_warmup:
                rec = WriteRecord(
                    run_id=run_id,
                    replication_factor=replication_factor,
                    concurrency=concurrency,
                    request_id=req_id,
                    doc_id=doc_id,
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
# Real Replica Verification (Dynamic Shard Discovery & Placement Aware)
# ---------------------------------------------------------------------------

def query_node_get(rpc_port: int, shard_id: int, doc_id: int, timeout: float = 5.0) -> Tuple[bool, bool, str]:
    """Queries /node/get for a specific (shard_id, doc_id).

    Returns (found, is_error, error_message).
    """
    try:
        conn = http.client.HTTPConnection(HOST, rpc_port, timeout=timeout)
        body = json.dumps({"shard_id": shard_id, "document_id": doc_id})
        conn.request("POST", "/node/get", body=body, headers={"Content-Type": "application/json"})
        resp = conn.getresponse()
        raw = resp.read().decode("utf-8", errors="replace")
        conn.close()
        if resp.status == 200:
            data = json.loads(raw)
            found = bool(data.get("found", False))
            is_err = bool(data.get("is_error", False))
            err_msg = str(data.get("error_message", ""))
            return found, is_err, err_msg
        else:
            return False, True, f"HTTP {resp.status}: {raw[:60]}"
    except Exception as exc:
        return False, True, str(exc)


def verify_replicas(
    doc_ids_sample: List[int],
    replication_factor: int,
    getter_fn=query_node_get,
) -> dict:
    """Verifies that each document in doc_ids_sample exists on its expected replica nodes:

      Dynamic Shard Discovery:
        Probes the three shard-primary locations:
          - Shard 0 -> Node 0 (RPC 9081)
          - Shard 1 -> Node 1 (RPC 9082)
          - Shard 2 -> Node 2 (RPC 9083)
        Exactly ONE candidate must return is_error=False AND found=True.
        Zero or multiple matches fail verification.

      Placement Verification:
        Once actual_shard_id is determined:
          expected_nodes = [(actual_shard_id + r) % 3 for r in range(replication_factor)]
        - For RF=1:
            expected_nodes = [actual_shard_id]
            Document must be found on Node actual_shard_id.
            The other two nodes must NOT contain the document (found=True on non-replica fails).
        - For RF=3:
            expected_nodes = [0, 1, 2]
            Document must be found on all three nodes for actual_shard_id.
    """
    nodes = [
        ("Node 0", 0, 9081),
        ("Node 1", 1, 9082),
        ("Node 2", 2, 9083),
    ]

    results = {
        "replication_factor": replication_factor,
        "sample_size": len(doc_ids_sample),
        "sample_ids": doc_ids_sample,
        "node_results": {},
        "missing_per_node": {name: [] for name, _, _ in nodes},
        "unexpected_per_node": {name: [] for name, _, _ in nodes},
        "discovery_errors": [],
        "all_verified": True,
    }

    if not doc_ids_sample:
        results["all_verified"] = False
        return results

    shard_primaries = [
        (0, 9081),  # Shard 0 primary is Node 0
        (1, 9082),  # Shard 1 primary is Node 1
        (2, 9083),  # Shard 2 primary is Node 2
    ]

    for doc_id in doc_ids_sample:
        # Step 1: Probe the three shard-primary locations
        matched_shards: List[int] = []
        for sid, rpc_port in shard_primaries:
            found, is_err, _ = getter_fn(rpc_port, sid, doc_id)
            if found and not is_err:
                matched_shards.append(sid)

        # Step 2: Determine actual_shard_id
        if len(matched_shards) == 0:
            results["all_verified"] = False
            results["discovery_errors"].append(
                f"doc_id {doc_id}: not found in any primary shard (0 matches during probe)"
            )
            continue
        elif len(matched_shards) > 1:
            results["all_verified"] = False
            results["discovery_errors"].append(
                f"doc_id {doc_id}: found in multiple primary shards {matched_shards} (ambiguous placement)"
            )
            continue

        actual_shard_id = matched_shards[0]

        # Step 3: Expected replica nodes based on actual_shard_id
        expected_node_ids = [(actual_shard_id + r) % 3 for r in range(replication_factor)]

        # Step 4: Verify across all 3 nodes
        for node_name, node_id, rpc_port in nodes:
            should_exist = node_id in expected_node_ids
            found, is_err, _ = getter_fn(rpc_port, actual_shard_id, doc_id)

            if should_exist:
                if not found or is_err:
                    results["missing_per_node"][node_name].append(doc_id)
            else:
                # For non-replicas, found=True is a violation.
                # Non-hosted shard error (is_err=True, found=False) is acceptable.
                if found:
                    results["unexpected_per_node"][node_name].append(doc_id)

    # Summarize per-node results
    for node_name, node_id, _ in nodes:
        missing = results["missing_per_node"][node_name]
        unexpected = results["unexpected_per_node"][node_name]

        if missing or unexpected:
            results["all_verified"] = False
            details = []
            if missing:
                details.append(f"missing: {missing}")
            if unexpected:
                details.append(f"unexpected: {unexpected}")
            results["node_results"][node_name] = f"FAILED ({'; '.join(details)})"
        else:
            results["node_results"][node_name] = "PASS"

    if results["discovery_errors"]:
        results["all_verified"] = False

    return results


# ---------------------------------------------------------------------------
# Benchmark Runner (Single Configuration Run)
# ---------------------------------------------------------------------------

def run_benchmark(
    replication_factor: int,
    concurrency: int,
    warmup_count: int,
    measured_count: int,
    run_id: str,
    out_prefix: str,
    port: int = DEFAULT_TARGET_PORT,
    host: str = HOST,
) -> Tuple[dict, List[WriteRecord]]:
    print(f"\n================================================================")
    print(f"Benchmark F - RF={replication_factor} Concurrency={concurrency} (Run ID: {run_id})")
    print(f"Target: http://{host}:{port}/documents")
    print(f"Workload: {measured_count} writes (Moderate payload) after {warmup_count} warmup")
    print(f"================================================================")

    # 1. Validate dataset readiness
    if not validate_queries(port=port, host=host):
        raise RuntimeError("Dataset validation failed: not all query terms return valid results.")

    # 2. Get ID ranges for this run
    rf_cfg = WRITE_ID_CONFIG.get(replication_factor, {}).get(concurrency, {
        "warmup_start": 3_000_000 + replication_factor * 200_000 + concurrency * 10_000 + 1,
        "measured_start": 3_000_000 + replication_factor * 200_000 + concurrency * 10_000 + 1001,
    })

    # 3. Warmup writes (round-robin partition)
    warmup_tasks = [(i, rf_cfg["warmup_start"] + i) for i in range(warmup_count)]
    warmup_chunks = [[] for _ in range(concurrency)]
    for i, t in enumerate(warmup_tasks):
        warmup_chunks[i % concurrency].append(t)

    print(f"  Running {warmup_count} warmup writes across {concurrency} workers (discarded)...")
    warmup_records: List[WriteRecord] = []
    warmup_lock = threading.Lock()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [
            ex.submit(
                _write_worker,
                chunk, warmup_records, warmup_lock, True,
                run_id, replication_factor, concurrency, host, port
            )
            for chunk in warmup_chunks if chunk
        ]
        for f in as_completed(futs):
            f.result()
    print(f"  Warmup complete ({warmup_count} writes discarded).")

    # 4. Snapshot metrics BEFORE measured writes
    metrics_before = get_metrics(port=port, host=host)
    coord_writes_before = metrics_before.get("coordinator_writes_total", 0)
    coord_write_success_before = metrics_before.get("coordinator_write_success", 0)
    coord_write_errors_before = metrics_before.get("coordinator_write_errors", 0)
    writes_total_before = metrics_before.get("writes_total", 0)
    write_errors_before = metrics_before.get("write_errors", 0)
    events_total_before = metrics_before.get("events_total", 0)
    events_pub_before = metrics_before.get("events_published", 0)
    events_failed_before = metrics_before.get("events_failed", 0)

    print(f"  Metrics BEFORE:")
    print(f"    coordinator_writes_total={coord_writes_before}  coordinator_write_success={coord_write_success_before}")
    print(f"    writes_total={writes_total_before}  events_published={events_pub_before}")

    # 5. Measured writes (round-robin partition)
    meas_tasks = [(i, rf_cfg["measured_start"] + i) for i in range(measured_count)]
    meas_chunks = [[] for _ in range(concurrency)]
    for i, t in enumerate(meas_tasks):
        meas_chunks[i % concurrency].append(t)

    print(f"  Running {measured_count} measured writes at concurrency={concurrency} (RF={replication_factor})...")
    records: List[WriteRecord] = []
    records_lock = threading.Lock()

    t_start = time.perf_counter()
    with ThreadPoolExecutor(max_workers=concurrency) as ex:
        futs = [
            ex.submit(
                _write_worker,
                chunk, records, records_lock, False,
                run_id, replication_factor, concurrency, host, port
            )
            for chunk in meas_chunks if chunk
        ]
        for f in as_completed(futs):
            f.result()
    t_end = time.perf_counter()
    elapsed_s = t_end - t_start

    # 6. Snapshot metrics AFTER measured writes
    metrics_after = get_metrics(port=port, host=host)
    coord_writes_after = metrics_after.get("coordinator_writes_total", 0)
    coord_write_success_after = metrics_after.get("coordinator_write_success", 0)
    coord_write_errors_after = metrics_after.get("coordinator_write_errors", 0)
    writes_total_after = metrics_after.get("writes_total", 0)
    write_errors_after = metrics_after.get("write_errors", 0)
    events_total_after = metrics_after.get("events_total", 0)
    events_pub_after = metrics_after.get("events_published", 0)
    events_failed_after = metrics_after.get("events_failed", 0)

    delta_coord_writes = coord_writes_after - coord_writes_before
    delta_coord_write_success = coord_write_success_after - coord_write_success_before
    delta_coord_write_errors = coord_write_errors_after - coord_write_errors_before
    delta_writes_total = writes_total_after - writes_total_before
    delta_write_errors = write_errors_after - write_errors_before
    delta_events_total = events_total_after - events_total_before
    delta_events_published = events_pub_after - events_pub_before
    delta_events_failed = events_failed_after - events_failed_before

    print(f"  Metrics AFTER:")
    print(f"    coordinator_writes_total={coord_writes_after}  coordinator_write_success={coord_write_success_after}")
    print(f"  Deltas:")
    print(f"    delta_coordinator_writes={delta_coord_writes} (success={delta_coord_write_success}, err={delta_coord_write_errors})")
    print(f"    delta_writes_total={delta_writes_total}  delta_events_published={delta_events_published}")

    # 7. Compute statistics
    records.sort(key=lambda r: r.request_id)
    total_requests = len(records)
    successful = sum(1 for r in records if r.success)
    errors = total_requests - successful
    latencies = [r.latency_ms for r in records if r.success]
    p50, p95, p99, mean_ms = compute_percentiles(latencies)
    throughput = total_requests / elapsed_s if elapsed_s > 0 else 0.0

    summary = {
        "run_id": run_id,
        "replication_factor": replication_factor,
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
        "delta_coordinator_writes": delta_coord_writes,
        "delta_coordinator_write_success": delta_coord_write_success,
        "delta_coordinator_write_errors": delta_coord_write_errors,
        "delta_writes_total": delta_writes_total,
        "delta_write_errors": delta_write_errors,
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
            "run_id", "replication_factor", "concurrency", "request_id",
            "doc_id", "status_code", "success", "latency_ms", "error"
        ])
        for r in records:
            writer.writerow([
                r.run_id, r.replication_factor, r.concurrency, r.request_id,
                r.doc_id, r.status_code, r.success, r.latency_ms, r.error
            ])

    # 9. Write per-concurrency summary CSV
    sum_csv = f"{out_prefix}_summary.csv"
    with open(sum_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(list(summary.keys()))
        writer.writerow(list(summary.values()))

    print(f"\n  Results: Requests={total_requests}  Success={successful}  Errors={errors}")
    print(f"  p50={summary['p50_ms']}ms  p95={summary['p95_ms']}ms  p99={summary['p99_ms']}ms  Mean={summary['mean_ms']}ms")
    print(f"  Elapsed={summary['elapsed_s']}s  Throughput={summary['throughput_req_s']} req/s")
    print(f"  Written: {req_csv}")
    print(f"  Written: {sum_csv}")

    return summary, records


# ---------------------------------------------------------------------------
# Consolidated Summary & Overhead Comparison Generation
# ---------------------------------------------------------------------------

def generate_consolidated_comparison(results_dir: str, out_csv: str) -> None:
    """Reads all RF=1 and RF=3 summary CSVs, computes overhead percentages,

    and outputs the consolidated comparison CSV.
    """
    concurrencies = [1, 2, 4, 8, 16]
    rf1_summaries: Dict[int, dict] = {}
    rf3_summaries: Dict[int, dict] = {}

    for c in concurrencies:
        f_rf1 = os.path.join(results_dir, f"phase21_F_rf1_c{c}_summary.csv")
        if os.path.exists(f_rf1):
            with open(f_rf1, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                if rows:
                    rf1_summaries[c] = rows[0]

        f_rf3 = os.path.join(results_dir, f"phase21_F_rf3_c{c}_summary.csv")
        if os.path.exists(f_rf3):
            with open(f_rf3, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                if rows:
                    rf3_summaries[c] = rows[0]

    header = [
        "replication_factor", "concurrency", "requests", "success", "errors",
        "p50_ms", "p95_ms", "p99_ms", "mean_ms", "throughput_rps",
        "p50_overhead_percent", "p95_overhead_percent", "p99_overhead_percent",
        "throughput_impact_percent"
    ]

    rows_out: List[dict] = []

    for c in concurrencies:
        s1 = rf1_summaries.get(c)
        s3 = rf3_summaries.get(c)

        if s1:
            rows_out.append({
                "replication_factor": 1,
                "concurrency": c,
                "requests": s1["requests"],
                "success": s1["successful"],
                "errors": s1["errors"],
                "p50_ms": s1["p50_ms"],
                "p95_ms": s1["p95_ms"],
                "p99_ms": s1["p99_ms"],
                "mean_ms": s1["mean_ms"],
                "throughput_rps": s1["throughput_req_s"],
                "p50_overhead_percent": "0.0",
                "p95_overhead_percent": "0.0",
                "p99_overhead_percent": "0.0",
                "throughput_impact_percent": "0.0",
            })

        if s3:
            p50_ovh = ""
            p95_ovh = ""
            p99_ovh = ""
            tput_imp = ""

            if s1:
                p50_1 = float(s1["p50_ms"])
                p50_3 = float(s3["p50_ms"])
                p95_1 = float(s1["p95_ms"])
                p95_3 = float(s3["p95_ms"])
                p99_1 = float(s1["p99_ms"])
                p99_3 = float(s3["p99_ms"])
                tput_1 = float(s1["throughput_req_s"])
                tput_3 = float(s3["throughput_req_s"])

                if p50_1 > 0:
                    p50_ovh = round(((p50_3 - p50_1) / p50_1) * 100.0, 4)
                if p95_1 > 0:
                    p95_ovh = round(((p95_3 - p95_1) / p95_1) * 100.0, 4)
                if p99_1 > 0:
                    p99_ovh = round(((p99_3 - p99_1) / p99_1) * 100.0, 4)
                if tput_1 > 0:
                    tput_imp = round(((tput_1 - tput_3) / tput_1) * 100.0, 4)

            rows_out.append({
                "replication_factor": 3,
                "concurrency": c,
                "requests": s3["requests"],
                "success": s3["successful"],
                "errors": s3["errors"],
                "p50_ms": s3["p50_ms"],
                "p95_ms": s3["p95_ms"],
                "p99_ms": s3["p99_ms"],
                "mean_ms": s3["mean_ms"],
                "throughput_rps": s3["throughput_req_s"],
                "p50_overhead_percent": p50_ovh,
                "p95_overhead_percent": p95_ovh,
                "p99_overhead_percent": p99_ovh,
                "throughput_impact_percent": tput_imp,
            })

    os.makedirs(os.path.dirname(out_csv) if os.path.dirname(out_csv) else ".", exist_ok=True)
    with open(out_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        for r in rows_out:
            writer.writerow(r)

    print(f"\nConsolidated replication comparison written to: {out_csv}")
    print("\n==========================================================================================")
    print("PHASE 21 BENCHMARK F: REPLICATION OVERHEAD COMPARISON")
    print("==========================================================================================")
    print(f"{'Concur':>6}  {'RF':>3}  {'p50 (ms)':>10}  {'p95 (ms)':>10}  {'p99 (ms)':>10}  {'Tput (r/s)':>11}  {'p50 Ovh %':>10}  {'p99 Ovh %':>10}  {'Tput Imp %':>11}")
    print("-" * 90)
    for r in rows_out:
        p50_ovh_str = f"{r['p50_overhead_percent']:>9}%" if r['p50_overhead_percent'] != "" else "      N/A"
        p99_ovh_str = f"{r['p99_overhead_percent']:>9}%" if r['p99_overhead_percent'] != "" else "      N/A"
        tput_imp_str = f"{r['throughput_impact_percent']:>10}%" if r['throughput_impact_percent'] != "" else "       N/A"
        print(
            f"{r['concurrency']:>6}  {r['replication_factor']:>3}  {float(r['p50_ms']):>10.3f}  {float(r['p95_ms']):>10.3f}  "
            f"{float(r['p99_ms']):>10.3f}  {float(r['throughput_rps']):>11.2f}  {p50_ovh_str}  {p99_ovh_str}  {tput_imp_str}"
        )
    print("==========================================================================================")


# ---------------------------------------------------------------------------
# Self-Tests (Verification Logic Unit / Mock Tests)
# ---------------------------------------------------------------------------

def run_self_tests() -> bool:
    """Focused unit tests for verify_replicas dynamic shard discovery and placement."""
    print("Running bench_F.py verification self-tests...")
    all_ok = True

    # Test Case 1: Document whose actual shard differs from doc_id % 3
    # doc_id 3011301: 3011301 % 3 == 0, but actual shard is 1 (Node 1)
    # RF=1: should expect only Node 1
    def mock_case1(rpc_port, shard_id, doc_id):
        # Shard 1 exists on Node 1 (9082)
        if shard_id == 1 and rpc_port == 9082:
            return True, False, ""
        if rpc_port == 9082 and shard_id != 1:
            return False, True, "Shard not found on node"
        if rpc_port == 9081:
            return False, (shard_id != 0), "Shard not found on node" if shard_id != 0 else ""
        if rpc_port == 9083:
            return False, (shard_id != 2), "Shard not found on node" if shard_id != 2 else ""
        return False, False, ""

    res1 = verify_replicas([3011301], replication_factor=1, getter_fn=mock_case1)
    if not res1["all_verified"] or res1["node_results"]["Node 1"] != "PASS":
        print("  [FAIL] Test 1: doc_id with actual shard != id % 3 (RF=1) failed", file=sys.stderr)
        all_ok = False
    else:
        print("  [PASS] Test 1: doc_id with actual shard != id % 3 (RF=1)")

    # Test Case 2: Zero shard probes succeed
    def mock_case2(rpc_port, shard_id, doc_id):
        return False, False, ""

    res2 = verify_replicas([9999999], replication_factor=1, getter_fn=mock_case2)
    if res2["all_verified"] or len(res2["discovery_errors"]) != 1:
        print("  [FAIL] Test 2: zero shard probes succeed should fail verification", file=sys.stderr)
        all_ok = False
    else:
        print("  [PASS] Test 2: zero shard probes succeed correctly rejected")

    # Test Case 3: Multiple shard probes succeed
    def mock_case3(rpc_port, shard_id, doc_id):
        if shard_id in (0, 1):
            return True, False, ""
        return False, False, ""

    res3 = verify_replicas([8888888], replication_factor=1, getter_fn=mock_case3)
    if res3["all_verified"] or len(res3["discovery_errors"]) != 1:
        print("  [FAIL] Test 3: multiple shard probes succeed should fail verification", file=sys.stderr)
        all_ok = False
    else:
        print("  [PASS] Test 3: multiple shard probes succeed correctly rejected")

    # Test Case 4: RF=1 unexpected placement (non-replica contains document)
    def mock_case4(rpc_port, shard_id, doc_id):
        if shard_id == 0:
            if rpc_port in (9081, 9082):
                return True, False, ""
        return False, False, ""

    res4 = verify_replicas([3011001], replication_factor=1, getter_fn=mock_case4)
    if res4["all_verified"] or 3011001 not in res4["unexpected_per_node"]["Node 1"]:
        print("  [FAIL] Test 4: RF=1 unexpected replica should fail verification", file=sys.stderr)
        all_ok = False
    else:
        print("  [PASS] Test 4: RF=1 unexpected replica correctly detected")

    # Test Case 5: RF=3 expected placement (all 3 nodes contain actual_shard_id)
    def mock_case5(rpc_port, shard_id, doc_id):
        if shard_id == 1:
            return True, False, ""
        return False, False, ""

    res5 = verify_replicas([3011301], replication_factor=3, getter_fn=mock_case5)
    if not res5["all_verified"] or not all(res5["node_results"][n] == "PASS" for n in ("Node 0", "Node 1", "Node 2")):
        print("  [FAIL] Test 5: RF=3 placement on all 3 nodes failed", file=sys.stderr)
        all_ok = False
    else:
        print("  [PASS] Test 5: RF=3 placement on all 3 nodes verified")

    # Test Case 6: RF=3 missing from one replica
    def mock_case6(rpc_port, shard_id, doc_id):
        if shard_id == 1:
            if rpc_port in (9081, 9082):
                return True, False, ""
            return False, False, ""
        return False, False, ""

    res6 = verify_replicas([3011301], replication_factor=3, getter_fn=mock_case6)
    if res6["all_verified"] or 3011301 not in res6["missing_per_node"]["Node 2"]:
        print("  [FAIL] Test 6: RF=3 missing replica should fail verification", file=sys.stderr)
        all_ok = False
    else:
        print("  [PASS] Test 6: RF=3 missing replica correctly detected")

    if all_ok:
        print("All self-tests passed successfully!")
    return all_ok


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Phase 21 Benchmark F - Replication Overhead Harness")
    parser.add_argument("--replica-factor", type=int, choices=[1, 3], help="Replication factor (1 or 3)")
    parser.add_argument("--concurrency", type=int, help="Worker concurrency level (1, 2, 4, 8, 16)")
    parser.add_argument("--run-id", type=str, help="Run identifier (e.g. F_rf1_c1)")
    parser.add_argument("--out-prefix", type=str, help="Output file prefix (e.g. results/phase21_F_rf1_c1)")
    parser.add_argument("--warmup", type=int, default=100, help="Number of warmup writes (default: 100)")
    parser.add_argument("--measured", type=int, default=500, help="Number of measured writes (default: 500)")
    parser.add_argument("--sample-size", type=int, default=10, help="Sample size for replica verification (default: 10)")
    parser.add_argument("--target-port", type=int, default=DEFAULT_TARGET_PORT, help="Target node HTTP port (default: 8081)")
    parser.add_argument("--prepare", action="store_true", help="Seed 510 baseline documents and validate query terms, then exit")
    parser.add_argument("--consolidate", action="store_true", help="Consolidate all RF=1 and RF=3 summaries and compute overhead")
    parser.add_argument("--results-dir", type=str, default="results", help="Directory containing summary CSVs (for --consolidate)")
    parser.add_argument("--out-consolidated", type=str, default="results/phase21_F_replication_overhead_consolidated.csv", help="Path for consolidated output CSV")
    parser.add_argument("--self-test", action="store_true", help="Run focused verification unit/mock tests and exit")
    args = parser.parse_args()

    if args.self_test:
        ok = run_self_tests()
        sys.exit(0 if ok else 1)

    if args.consolidate:
        generate_consolidated_comparison(args.results_dir, args.out_consolidated)
        sys.exit(0)

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

    if not args.replica_factor or not args.concurrency or not args.run_id or not args.out_prefix:
        print("ERROR: --replica-factor, --concurrency, --run-id, and --out-prefix are required for benchmark runs.", file=sys.stderr)
        sys.exit(1)

    summary, records = run_benchmark(
        replication_factor=args.replica_factor,
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
    successful_ids = [r.doc_id for r in records if r.success]
    sample_size = min(args.sample_size, len(successful_ids))
    if sample_size > 0:
        step = max(1, len(successful_ids) // sample_size)
        doc_ids_sample = [successful_ids[i * step] for i in range(sample_size)]
    else:
        doc_ids_sample = []

    print("\n--- REPLICA VERIFICATION ---")
    print(f"Replication Factor: {args.replica_factor}")
    print(f"Sample size: {len(doc_ids_sample)}")
    print(f"Sample IDs: {doc_ids_sample}")

    verification = verify_replicas(doc_ids_sample, args.replica_factor)
    print(f"Node 0 verification: {verification['node_results'].get('Node 0', 'N/A')}")
    print(f"Node 1 verification: {verification['node_results'].get('Node 1', 'N/A')}")
    print(f"Node 2 verification: {verification['node_results'].get('Node 2', 'N/A')}")
    if verification.get("discovery_errors"):
        for err in verification["discovery_errors"]:
            print(f"  Discovery error: {err}", file=sys.stderr)

    if not verification["all_verified"]:
        print(f"\nERROR: Replica verification FAILED for RF={args.replica_factor}!", file=sys.stderr)
        for node_name, missing_list in verification["missing_per_node"].items():
            if missing_list:
                print(f"  {node_name}: missing IDs {missing_list}", file=sys.stderr)
        for node_name, unexp_list in verification["unexpected_per_node"].items():
            if unexp_list:
                print(f"  {node_name}: unexpected IDs {unexp_list}", file=sys.stderr)
        sys.exit(3)

    print(f"\nReplica verification: PASS (all sampled IDs match expected placement for RF={args.replica_factor})")
    sys.exit(0)


if __name__ == "__main__":
    main()
