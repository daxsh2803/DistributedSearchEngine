#!/usr/bin/env python3
"""
Phase 21 Benchmark I — Kafka Unavailability: Write-Path Resilience.

Goal:
  Measure the resilience and availability of the authoritative synchronous write path
  when the asynchronous Kafka event broker is completely unavailable.

Architectural Principles & Invariants:
  1. Authoritative Write Path:
     POST /documents -> ShardCoordinator.ingest()
                     -> Synchronous RF=3 replication across cluster
                     -> HTTP 201 Created returned to client
                     -> EventStore.add() (status: PENDING)
                     -> EventDispatcher background thread
                     -> KafkaMessageBroker.publish() -> Kafka

  2. Asynchronous Decoupling:
     Kafka publication is strictly asynchronous and decoupled from the write response.
     Kafka unavailability MUST NOT cause client write requests to fail once synchronous
     RF=3 replication has succeeded.

  3. Bounded Event Failure & No Auto-Replay:
     While Kafka is down, EventDispatcher retries failed publications up to a bounded limit
     (3 retries with exponential/fixed backoff). When retries are exhausted, the event is
     marked as FAILED in EventStore, incrementing `events_failed`.
     CRITICAL LIMITATION: EventDispatcher::replay_failed() exists in C++, but there is
     NO HTTP/admin endpoint exposing it. Therefore Benchmark I MUST NOT attempt to replay
     the 50 failed Phase-2 events. Those events remain in FAILED status.
     Phase 3 tests only NEW writes after Kafka recovery.

  4. Recovery Assertion:
     The benchmark MUST NOT assert `final events_failed == 0`.
     Expected final failed count is approximately:
         baseline_failed + 50
     The correct recovery assertion is:
         recovery_failed_delta == 0
     (zero new failures for writes issued after Kafka recovery).

Benchmark Phases:
  Phase 1 — Kafka healthy (Baseline):
    - 10 warmup writes to Node 0 (discarded)
    - Capture baseline /metrics
    - Capture Kafka consumer group offset baseline for all 3 groups

  Phase 2 — Kafka unavailable:
    - Stop Docker container `dse-kafka`
    - Verify Kafka is unreachable on port 9094
    - Issue 50 sequential measured writes to Node 0 (IDs 9_001_001 to 9_001_050)
    - Expect HTTP 201 for ALL 50 writes
    - Verify representative documents exist on all 3 replicas (RPC /node/get)
    - Wait 5-second buffer for EventDispatcher bounded retries to exhaust
    - Capture /metrics: verify `events_failed_delta == 50` and `events_published_delta == 0`

  Phase 3 — Kafka recovered:
    - Start Docker container `dse-kafka`
    - Wait for Kafka TCP reachability on localhost:9094 (timeout 60s)
    - Wait for all 3 consumer groups to recover (timeout 30s)
    - Record pre-recovery metrics snapshot
    - Issue 10 NEW sequential writes to Node 0 (IDs 9_002_001 to 9_002_010)
    - Expect HTTP 201 for ALL 10 writes
    - Verify representative recovery documents on all 3 replicas
    - Wait for recovery events to publish (poll metrics, timeout 30s)
    - Verify `recovery_failed_delta == 0` (zero new failures during Phase 3)
    - Verify consumer lag across all 3 groups returns to zero
    - Verify final document consistency across all replicas
"""

import argparse
import csv
import http.client
import json
import os
import socket
import statistics
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

# ---------------------------------------------------------------------------
# Constants & Configuration
# ---------------------------------------------------------------------------

HOST = "127.0.0.1"
DEFAULT_TARGET_PORT = 8081  # Node 0 HTTP
DEFAULT_RESULTS_DIR = os.path.join("results", "phase21_I_kafka_unavail")
DEFAULT_RUN_ID = "I_kafka_unavail"

KAFKA_BOOTSTRAP_HOST = "localhost"
KAFKA_BOOTSTRAP_PORT = 9094
KAFKA_CONTAINER = "dse-kafka"
KAFKA_INTERNAL_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "documents.mutations"
PARTITIONS = [0, 1, 2]
CONSUMER_GROUPS = ["dse-node-0", "dse-node-1", "dse-node-2"]

# Cluster node ports
CLUSTER_NODES = [
    {"id": 0, "http": 8081, "rpc": 9081},
    {"id": 1, "http": 8082, "rpc": 9082},
    {"id": 2, "http": 8083, "rpc": 9083},
]

# Workload configuration
DEFAULT_WARMUP_WRITES = 10
DEFAULT_MEASURED_WRITES = 50
DEFAULT_RECOVERY_WRITES = 10

# Distinct document ID ranges starting at 9_000_000+ (zero collision)
ID_WARMUP_START = 9_000_001    # 9_000_001 .. 9_000_010
ID_MEASURED_START = 9_001_001  # 9_001_001 .. 9_001_050
ID_RECOVERY_START = 9_002_001  # 9_002_001 .. 9_002_010

# Timings & Timeouts (seconds)
DEFAULT_DRAIN_BUFFER_SEC = 5.0       # Wait for EventDispatcher retries to exhaust after Phase 2
DEFAULT_KAFKA_TIMEOUT_SEC = 60.0     # Max wait for Kafka restart reachability
DEFAULT_RECOVERY_DRAIN_SEC = 30.0    # Max wait for Phase 3 events to publish and lag to zero
DEFAULT_KAFKA_CLI_TIMEOUT_SEC = 10.0 # Per-query timeout for kafka-consumer-groups.sh

# Exit codes
EXIT_PASS = 0
EXIT_WORKLOAD_FAILURE = 1
EXIT_INFRA_FAILURE = 2
EXIT_VERIFICATION_FAILURE = 3

CONTENT_M = (
    "distributed search engines index large document collections using "
    "inverted index structures with tf-idf scoring algorithms for ranked "
    "retrieval supporting boolean query modes including or and and with "
    "concurrent multi-shard fan-out across replicated node clusters"
)


# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------

@dataclass
class WriteRecord:
    phase: str
    request_id: int
    doc_id: int
    http_status: int
    success: bool
    latency_ms: float
    error: str = ""


@dataclass
class MetricsSnapshot:
    snapshot_id: int
    timestamp_rel_s: float
    phase: str
    events_total: Optional[int]
    events_pending: Optional[int]
    events_dispatching: Optional[int]
    events_published: Optional[int]
    events_failed: Optional[int]
    events_retried: Optional[int]
    events_replayed: Optional[int]
    writes_total: Optional[int]
    write_errors: Optional[int]
    coordinator_writes_total: Optional[int]
    coordinator_write_success: Optional[int]
    coordinator_write_errors: Optional[int]


@dataclass
class PartitionLagRecord:
    group: str
    partition: int
    current_offset: Optional[int]
    log_end_offset: Optional[int]
    lag: Optional[int]
    query_failed: bool = False
    error_msg: str = ""


@dataclass
class LagSnapshot:
    sample_index: int
    timestamp_rel_s: float
    phase: str
    records: List[PartitionLagRecord]
    query_success: bool = True
    error_msg: str = ""

    def group_lag(self, group: str) -> Optional[int]:
        g_recs = [r for r in self.records if r.group == group]
        if not g_recs or any(r.query_failed or r.lag is None for r in g_recs):
            return None
        return sum(r.lag for r in g_recs if r.lag is not None)

    def total_lag(self) -> Optional[int]:
        if not self.query_success or not self.records:
            return None
        if any(r.query_failed or r.lag is None for r in self.records):
            return None
        return sum(r.lag for r in self.records if r.lag is not None)


# ---------------------------------------------------------------------------
# Statistical Calculations
# ---------------------------------------------------------------------------

def compute_percentiles(samples: List[float]) -> Dict[str, float]:
    if not samples:
        return {"min": 0.0, "p50": 0.0, "p95": 0.0, "p99": 0.0, "mean": 0.0, "max": 0.0}
    s = sorted(samples)
    n = len(s)
    if n >= 2:
        q = statistics.quantiles(s, n=100, method="inclusive")
        p50 = q[49]
        p95 = q[94]
        p99 = q[98]
    else:
        p50 = s[0]
        p95 = s[0]
        p99 = s[0]
    return {
        "min": round(s[0], 3),
        "p50": round(p50, 3),
        "p95": round(p95, 3),
        "p99": round(p99, 3),
        "mean": round(statistics.mean(s), 3),
        "max": round(s[-1], 3),
    }


def compute_metric_deltas(
    before: Dict[str, Any], after: Dict[str, Any]
) -> Dict[str, int]:
    """Computes differences for all numeric event and write metrics."""
    deltas: Dict[str, int] = {}
    keys = [
        "events_total", "events_pending", "events_dispatching",
        "events_published", "events_failed", "events_retried", "events_replayed",
        "writes_total", "write_errors",
        "coordinator_writes_total", "coordinator_write_success", "coordinator_write_errors"
    ]
    for k in keys:
        b_val = before.get(k, 0)
        a_val = after.get(k, 0)
        if b_val is not None and a_val is not None:
            deltas[f"{k}_delta"] = a_val - b_val
        else:
            deltas[f"{k}_delta"] = 0
    return deltas


def compute_recovery_failed_delta(
    phase3_pre: Dict[str, Any], phase3_post: Dict[str, Any]
) -> int:
    """
    Computes recovery_failed_delta.
    The assertion is recovery_failed_delta == 0 (zero new failures during Phase 3).
    """
    failed_before = phase3_pre.get("events_failed", 0) or 0
    failed_after = phase3_post.get("events_failed", 0) or 0
    return failed_after - failed_before


def compute_expected_final_failed(
    baseline_failed: int, phase2_write_count: int
) -> int:
    """Expected final failed events count = baseline_failed + phase2_write_count."""
    return baseline_failed + phase2_write_count


def evaluate_final_lag(
    final_lag: Optional[int],
    snapshot: Optional[LagSnapshot] = None
) -> Tuple[bool, str]:
    """
    Evaluates whether final Kafka consumer lag successfully reached zero.
    Rules:
      - query failure / unknown lag => FAIL
      - total_lag is None => FAIL
      - total_lag != 0 => FAIL
      - total_lag == 0 (with query_success=True) => PASS
    Returns:
      (passed: bool, message: str)
    """
    if snapshot is None and final_lag is None:
        return False, "Final Kafka consumer lag is unknown (no lag snapshot collected)"
    if snapshot is not None and not snapshot.query_success:
        return False, f"Kafka lag query failed: total_lag is {final_lag}"
    if final_lag is None:
        return False, "Final Kafka consumer lag is unknown (total_lag is None)"
    if final_lag != 0:
        return False, f"Final Kafka consumer lag did not reach zero: remaining lag = {final_lag}"
    return True, "Final Kafka consumer lag reached zero (total_lag = 0)"


# ---------------------------------------------------------------------------
# Freshness & Overwrite Guard
# ---------------------------------------------------------------------------

def check_result_freshness(file_paths: List[str], allow_overwrite: bool = False) -> None:
    """Refuses execution if any target output file already exists."""
    if allow_overwrite:
        return
    existing = [p for p in file_paths if os.path.exists(p)]
    if existing:
        raise FileExistsError(
            f"Target benchmark result files already exist: {existing}. "
            "Never overwrite historical benchmark output automatically. Refusing execution."
        )


# ---------------------------------------------------------------------------
# Network & Probing Primitives
# ---------------------------------------------------------------------------

def check_tcp_port(host: str, port: int, timeout: float = 1.0) -> bool:
    """Validates real TCP socket connect/connected semantics."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, socket.timeout):
        return False


def check_http_health(port: int, timeout: float = 2.0) -> bool:
    """Checks GET /health on localhost:port."""
    try:
        conn = http.client.HTTPConnection(HOST, port, timeout=timeout)
        conn.request("GET", "/health")
        resp = conn.getresponse()
        resp.read()
        conn.close()
        return resp.status == 200
    except Exception:
        return False


def get_node_metrics(port: int, timeout: float = 2.0) -> Dict[str, Any]:
    """Retrieves and parses GET /metrics JSON payload."""
    try:
        conn = http.client.HTTPConnection(HOST, port, timeout=timeout)
        conn.request("GET", "/metrics")
        resp = conn.getresponse()
        if resp.status == 200:
            data = resp.read()
            conn.close()
            return json.loads(data)
        conn.close()
    except Exception:
        pass
    return {}


# ---------------------------------------------------------------------------
# HTTP Document Operations
# ---------------------------------------------------------------------------

def send_write_request(
    port: int,
    doc_id: int,
    content: str,
    timeout_s: float = 10.0
) -> Tuple[int, float, str]:
    """Sends POST /documents request to target node and returns (status, lat_ms, err_msg)."""
    payload = json.dumps({
        "id": doc_id,
        "title": f"Bench I Document {doc_id}",
        "content": content
    }).encode("utf-8")

    headers = {
        "Content-Type": "application/json",
        "Content-Length": str(len(payload))
    }

    t0 = time.perf_counter()
    try:
        conn = http.client.HTTPConnection(HOST, port, timeout=timeout_s)
        conn.request("POST", "/documents", body=payload, headers=headers)
        resp = conn.getresponse()
        resp.read()
        conn.close()
        lat_ms = (time.perf_counter() - t0) * 1000.0
        return resp.status, lat_ms, ""
    except Exception as e:
        lat_ms = (time.perf_counter() - t0) * 1000.0
        return 0, lat_ms, str(e)


def verify_document_on_node(
    rpc_port: int,
    doc_id: int,
    timeout_s: float = 5.0
) -> bool:
    """
    Checks if a document is present locally on a specific node via RPC POST /node/get.
    Queries candidate shards [doc_id % 3, 0, 1, 2].
    """
    candidate_shards = [doc_id % 3] + [s for s in range(3) if s != doc_id % 3]
    for sid in candidate_shards:
        try:
            conn = http.client.HTTPConnection(HOST, rpc_port, timeout=timeout_s)
            body = json.dumps({"shard_id": sid, "document_id": doc_id})
            conn.request("POST", "/node/get", body=body, headers={"Content-Type": "application/json"})
            resp = conn.getresponse()
            raw = resp.read().decode("utf-8", errors="replace")
            conn.close()
            if resp.status == 200:
                data = json.loads(raw)
                if data.get("found", False) is True:
                    return True
        except Exception:
            pass
    return False


def verify_document_replicas(
    doc_ids: List[int],
    nodes: Optional[List[Dict[str, Any]]] = None
) -> Tuple[bool, Dict[str, Any]]:
    """
    Verifies that all specified document IDs exist locally on all cluster node replicas.
    """
    cluster = nodes or CLUSTER_NODES
    all_ok = True
    details: Dict[str, Any] = {}

    for n in cluster:
        node_id = n["id"]
        rpc_port = n["rpc"]
        missing = []
        found_cnt = 0
        for doc_id in doc_ids:
            if verify_document_on_node(rpc_port, doc_id):
                found_cnt += 1
            else:
                missing.append(doc_id)
                all_ok = False
        details[f"node_{node_id}"] = {
            "verified": found_cnt,
            "total": len(doc_ids),
            "missing": missing
        }

    return all_ok, details


# ---------------------------------------------------------------------------
# Kafka CLI Queries & Parsing
# ---------------------------------------------------------------------------

def parse_consumer_group_describe_output(
    group_name: str, stdout_text: str, topic_name: str = KAFKA_TOPIC
) -> Tuple[List[PartitionLagRecord], bool, str]:
    """
    Parses output of `kafka-consumer-groups.sh --describe --group <group>`.
    Correctness: Never converts query failures into lag=0.
    Handles CURRENT-OFFSET '-' as uncommitted lag = LOG-END-OFFSET.
    """
    records: List[PartitionLagRecord] = []
    seen_partitions: Set[int] = set()
    has_parse_error = False
    parse_error_msg = ""

    for line in stdout_text.splitlines():
        line = line.strip()
        if not line:
            continue
        if line.startswith("GROUP") or "CURRENT-OFFSET" in line or "LOG-END-OFFSET" in line:
            continue
        if line.startswith("Consumer group") or line.startswith("WARN"):
            continue
        if line.startswith("Error:"):
            has_parse_error = True
            parse_error_msg = line
            continue

        parts = line.split()
        if len(parts) < 5:
            continue

        grp = parts[0]
        tpc = parts[1]
        if grp != group_name or tpc != topic_name:
            continue

        try:
            partition = int(parts[2])
        except ValueError:
            continue

        cur_str = parts[3]
        cur_offset: Optional[int] = None
        if cur_str.isdigit():
            cur_offset = int(cur_str)
        elif cur_str != "-":
            has_parse_error = True
            parse_error_msg = f"Invalid CURRENT-OFFSET: '{cur_str}'"

        end_str = parts[4]
        end_offset: Optional[int] = None
        if end_str.isdigit():
            end_offset = int(end_str)
        else:
            has_parse_error = True
            parse_error_msg = f"Invalid LOG-END-OFFSET: '{end_str}'"

        lag: Optional[int] = None
        if len(parts) >= 6 and parts[5].isdigit():
            lag = int(parts[5])
        elif end_offset is not None:
            if cur_offset is not None:
                lag = max(0, end_offset - cur_offset)
            else:
                lag = max(0, end_offset)
        else:
            has_parse_error = True
            lag = None

        records.append(PartitionLagRecord(
            group=group_name,
            partition=partition,
            current_offset=cur_offset,
            log_end_offset=end_offset,
            lag=lag,
            query_failed=(lag is None),
            error_msg=parse_error_msg if lag is None else ""
        ))
        seen_partitions.add(partition)

    missing_partitions = [p for p in PARTITIONS if p not in seen_partitions]
    if missing_partitions:
        has_parse_error = True
        err = f"Partitions {missing_partitions} missing from Kafka CLI output"
        for p in missing_partitions:
            records.append(PartitionLagRecord(
                group=group_name, partition=p,
                current_offset=None, log_end_offset=None, lag=None,
                query_failed=True, error_msg=err
            ))

    records.sort(key=lambda r: r.partition)
    success = (not has_parse_error) and (len(seen_partitions) == len(PARTITIONS))
    err_summary = parse_error_msg if not success else ""
    return records, success, err_summary


def query_group_offsets(
    group_name: str,
    timeout_s: float = DEFAULT_KAFKA_CLI_TIMEOUT_SEC,
    topic_name: str = KAFKA_TOPIC,
    container_name: str = KAFKA_CONTAINER,
    bootstrap_server: str = KAFKA_INTERNAL_BOOTSTRAP
) -> Tuple[List[PartitionLagRecord], bool, str]:
    """Executes kafka-consumer-groups.sh for a group. Never converts failure to lag=0."""
    cmd = [
        "docker", "exec", container_name,
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server", bootstrap_server,
        "--describe",
        "--group", group_name
    ]

    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout_s)
        if proc.returncode != 0:
            err = proc.stderr.strip() or f"CLI returned code {proc.returncode}"
            return [
                PartitionLagRecord(
                    group=group_name, partition=p,
                    current_offset=None, log_end_offset=None, lag=None,
                    query_failed=True, error_msg=err
                )
                for p in PARTITIONS
            ], False, err

        records, success, err = parse_consumer_group_describe_output(group_name, proc.stdout, topic_name)
        return records, success, err

    except subprocess.TimeoutExpired:
        err = f"Kafka CLI query timed out after {timeout_s}s"
        return [
            PartitionLagRecord(
                group=group_name, partition=p,
                current_offset=None, log_end_offset=None, lag=None,
                query_failed=True, error_msg=err
            )
            for p in PARTITIONS
        ], False, err

    except Exception as e:
        err = f"Subprocess invocation error: {e}"
        return [
            PartitionLagRecord(
                group=group_name, partition=p,
                current_offset=None, log_end_offset=None, lag=None,
                query_failed=True, error_msg=err
            )
            for p in PARTITIONS
        ], False, err


def collect_lag_snapshot(
    sample_index: int,
    timestamp_rel_s: float,
    phase: str,
    groups: Optional[List[str]] = None,
    cli_timeout_s: float = DEFAULT_KAFKA_CLI_TIMEOUT_SEC,
    query_fn: Optional[Callable[[str], Tuple[List[PartitionLagRecord], bool, str]]] = None
) -> LagSnapshot:
    """Queries all 3 consumer groups concurrently with deterministic sorting."""
    target_groups = groups or CONSUMER_GROUPS
    group_results: Dict[str, Tuple[List[PartitionLagRecord], bool, str]] = {}

    def query_task(g: str) -> Tuple[str, Tuple[List[PartitionLagRecord], bool, str]]:
        if query_fn:
            return g, query_fn(g)
        return g, query_group_offsets(g, timeout_s=cli_timeout_s)

    with ThreadPoolExecutor(max_workers=len(target_groups)) as pool:
        futures = [pool.submit(query_task, g) for g in target_groups]
        for f in as_completed(futures):
            g, res = f.result()
            group_results[g] = res

    all_records: List[PartitionLagRecord] = []
    all_success = True
    err_msgs: List[str] = []

    for g in sorted(target_groups):
        records, success, err = group_results.get(g, ([], False, "Missing group result"))
        if not success:
            all_success = False
            if err:
                err_msgs.append(f"{g}: {err}")
        all_records.extend(records)

    all_records.sort(key=lambda r: (r.group, r.partition))
    return LagSnapshot(
        sample_index=sample_index,
        timestamp_rel_s=timestamp_rel_s,
        phase=phase,
        records=all_records,
        query_success=all_success,
        error_msg="; ".join(err_msgs) if err_msgs else ""
    )


# ---------------------------------------------------------------------------
# Docker Container Control & Reachability
# ---------------------------------------------------------------------------

def docker_stop_kafka(container: str = KAFKA_CONTAINER, timeout_s: float = 30.0) -> Tuple[bool, str]:
    """Stops the Kafka docker container and verifies port 9094 is unreachable."""
    try:
        proc = subprocess.run(["docker", "stop", container], capture_output=True, text=True, timeout=timeout_s)
        if proc.returncode != 0:
            return False, f"docker stop failed: {proc.stderr.strip()}"
    except Exception as e:
        return False, f"docker stop exception: {e}"

    # Verify reachability drops
    deadline = time.monotonic() + 5.0
    while time.monotonic() < deadline:
        if not check_tcp_port(KAFKA_BOOTSTRAP_HOST, KAFKA_BOOTSTRAP_PORT, timeout=0.5):
            return True, "Kafka container stopped and port 9094 unreachable"
        time.sleep(0.3)

    return False, "Port 9094 still accepting connections after docker stop"


def docker_start_kafka(container: str = KAFKA_CONTAINER, timeout_s: float = 30.0) -> Tuple[bool, str]:
    """Starts the Kafka docker container."""
    try:
        proc = subprocess.run(["docker", "start", container], capture_output=True, text=True, timeout=timeout_s)
        if proc.returncode != 0:
            return False, f"docker start failed: {proc.stderr.strip()}"
        return True, "docker start completed"
    except Exception as e:
        return False, f"docker start exception: {e}"


def wait_for_kafka_ready(
    host: str = KAFKA_BOOTSTRAP_HOST,
    port: int = KAFKA_BOOTSTRAP_PORT,
    timeout_s: float = DEFAULT_KAFKA_TIMEOUT_SEC
) -> Tuple[bool, float]:
    """Waits for Kafka TCP reachability using monotonic clock."""
    t0 = time.monotonic()
    deadline = t0 + timeout_s
    while time.monotonic() < deadline:
        if check_tcp_port(host, port, timeout=1.0) or check_tcp_port("127.0.0.1", port, timeout=1.0):
            elapsed = time.monotonic() - t0
            return True, elapsed
        time.sleep(0.5)
    return False, time.monotonic() - t0


def wait_for_consumer_groups_ready(
    timeout_s: float = 30.0,
    container: str = KAFKA_CONTAINER,
    groups: Optional[List[str]] = None
) -> Tuple[bool, str]:
    """Waits for all 3 consumer groups to be active in the restored Kafka broker."""
    target_groups = groups or CONSUMER_GROUPS
    t0 = time.monotonic()
    deadline = t0 + timeout_s
    while time.monotonic() < deadline:
        try:
            res = subprocess.run(
                ["docker", "exec", container, "/opt/kafka/bin/kafka-consumer-groups.sh",
                 "--bootstrap-server", KAFKA_INTERNAL_BOOTSTRAP, "--list"],
                capture_output=True, text=True, timeout=5.0
            )
            if res.returncode == 0:
                active = set(line.strip() for line in res.stdout.splitlines() if line.strip())
                missing = [g for g in target_groups if g not in active]
                if not missing:
                    return True, "All consumer groups active"
        except Exception:
            pass
        time.sleep(1.0)
    return False, f"Consumer groups not registered after {timeout_s}s: {target_groups}"


# ---------------------------------------------------------------------------
# Core Benchmark Workload Logic
# ---------------------------------------------------------------------------

def run_benchmark_i(
    target_port: int = DEFAULT_TARGET_PORT,
    warmup_count: int = DEFAULT_WARMUP_WRITES,
    measured_count: int = DEFAULT_MEASURED_WRITES,
    recovery_count: int = DEFAULT_RECOVERY_WRITES,
    drain_buffer_s: float = DEFAULT_DRAIN_BUFFER_SEC,
    kafka_timeout_s: float = DEFAULT_KAFKA_TIMEOUT_SEC,
    recovery_drain_s: float = DEFAULT_RECOVERY_DRAIN_SEC,
    cli_timeout_s: float = DEFAULT_KAFKA_CLI_TIMEOUT_SEC,
    results_dir: str = DEFAULT_RESULTS_DIR,
    run_id: str = DEFAULT_RUN_ID,
    out_prefix: Optional[str] = None
) -> int:
    """Executes the complete 3-phase Benchmark I workload."""
    os.makedirs(results_dir, exist_ok=True)
    prefix = out_prefix or os.path.join(results_dir, f"phase21_{run_id}")
    summary_path = f"{prefix}_summary.csv"
    writes_path = f"{prefix}_writes.csv"
    metrics_path = f"{prefix}_metrics_timeline.csv"

    # Upfront Freshness Check
    check_result_freshness([summary_path, writes_path, metrics_path])

    print("================================================================")
    print("Phase 21 Benchmark I — Kafka Unavailability: Write-Path Resilience")
    print("================================================================")
    print(f"Target Node 0 HTTP: {HOST}:{target_port}")
    print(f"Workload: {warmup_count} warmup, {measured_count} Kafka-unavailable, {recovery_count} recovery")
    print(f"Summary CSV:  {summary_path}")
    print(f"Writes CSV:   {writes_path}")
    print(f"Metrics CSV:  {metrics_path}")
    print("----------------------------------------------------------------")

    # Verify cluster health upfront
    for n in CLUSTER_NODES:
        if not check_http_health(n["http"]):
            print(f"ERROR: Node {n['id']} HTTP ({n['http']}) is not healthy!", file=sys.stderr)
            return EXIT_INFRA_FAILURE

    t_bench_start = time.monotonic()
    all_writes: List[WriteRecord] = []
    all_metrics: List[MetricsSnapshot] = []
    metric_snap_id = 0

    def capture_metrics_snap(phase: str) -> MetricsSnapshot:
        nonlocal metric_snap_id
        metric_snap_id += 1
        rel_t = time.monotonic() - t_bench_start
        m = get_node_metrics(target_port)
        snap = MetricsSnapshot(
            snapshot_id=metric_snap_id,
            timestamp_rel_s=round(rel_t, 3),
            phase=phase,
            events_total=m.get("events_total"),
            events_pending=m.get("events_pending"),
            events_dispatching=m.get("events_dispatching"),
            events_published=m.get("events_published"),
            events_failed=m.get("events_failed"),
            events_retried=m.get("events_retried"),
            events_replayed=m.get("events_replayed"),
            writes_total=m.get("writes_total"),
            write_errors=m.get("write_errors"),
            coordinator_writes_total=m.get("coordinator_writes_total"),
            coordinator_write_success=m.get("coordinator_write_success"),
            coordinator_write_errors=m.get("coordinator_write_errors"),
        )
        all_metrics.append(snap)
        return snap

    capture_metrics_snap("cluster_init")

    # =========================================================================
    # PHASE 1 — Kafka Healthy (Baseline)
    # =========================================================================
    print("\n--- Phase 1: Kafka Healthy (Warmup & Baseline) ---")
    req_id = 0
    for i in range(warmup_count):
        req_id += 1
        doc_id = ID_WARMUP_START + i
        status, lat_ms, err = send_write_request(target_port, doc_id, CONTENT_M)
        all_writes.append(WriteRecord(
            phase="phase1_warmup", request_id=req_id, doc_id=doc_id,
            http_status=status, success=(status == 201), latency_ms=lat_ms, error=err
        ))
        if status != 201:
            print(f"ERROR: Warmup write failed (doc_id={doc_id}, status={status}, err={err})", file=sys.stderr)
            return EXIT_WORKLOAD_FAILURE

    time.sleep(1.0)  # Brief settle pause
    snap_p1_baseline = capture_metrics_snap("phase1_baseline")
    m_baseline = get_node_metrics(target_port)
    baseline_failed = m_baseline.get("events_failed", 0) or 0
    baseline_published = m_baseline.get("events_published", 0) or 0
    print(f"Phase 1 complete: 10 warmup writes OK. Baseline events_failed={baseline_failed}, published={baseline_published}")

    snap_kafka_baseline = collect_lag_snapshot(0, time.monotonic() - t_bench_start, "phase1_baseline", cli_timeout_s=cli_timeout_s)
    print(f"Kafka baseline: query_success={snap_kafka_baseline.query_success}, total_lag={snap_kafka_baseline.total_lag()}")

    # =========================================================================
    # PHASE 2 — Kafka Unavailable (Write Availability & Failure Resilience)
    # =========================================================================
    print("\n--- Phase 2: Kafka Unavailable ---")
    print(f"Stopping container '{KAFKA_CONTAINER}'...")
    stop_ok, stop_msg = docker_stop_kafka(KAFKA_CONTAINER)
    if not stop_ok:
        print(f"ERROR: Failed to stop Kafka: {stop_msg}", file=sys.stderr)
        return EXIT_INFRA_FAILURE
    print(f"Kafka stopped successfully ({stop_msg}).")
    capture_metrics_snap("phase2_kafka_stopped")

    print(f"Issuing {measured_count} sequential measured writes to Node 0 while Kafka is down...")
    phase2_latencies: List[float] = []
    phase2_failures: List[WriteRecord] = []
    p2_doc_ids: List[int] = []

    for i in range(measured_count):
        req_id += 1
        doc_id = ID_MEASURED_START + i
        p2_doc_ids.append(doc_id)
        status, lat_ms, err = send_write_request(target_port, doc_id, CONTENT_M)
        rec = WriteRecord(
            phase="phase2_kafka_down", request_id=req_id, doc_id=doc_id,
            http_status=status, success=(status == 201), latency_ms=lat_ms, error=err
        )
        all_writes.append(rec)
        if status == 201:
            phase2_latencies.append(lat_ms)
        else:
            phase2_failures.append(rec)

    phase2_success_cnt = len(phase2_latencies)
    phase2_fail_cnt = len(phase2_failures)
    print(f"Phase 2 writes completed: {phase2_success_cnt}/{measured_count} HTTP 201 successes, {phase2_fail_cnt} failures.")

    if phase2_fail_cnt > 0:
        print(f"ERROR: Write availability violated during Kafka outage! {phase2_fail_cnt} writes failed.", file=sys.stderr)
        return EXIT_WORKLOAD_FAILURE

    # Verify representative documents on all 3 replicas
    # Pick 5 evenly spaced documents from the 50 measured writes
    sample_indices = [0, 9, 24, 39, 49]
    sample_phase2_ids = [p2_doc_ids[idx] for idx in sample_indices if idx < len(p2_doc_ids)]
    print(f"Verifying Phase 2 representative documents on all 3 replicas: {sample_phase2_ids}...")
    p2_replica_ok, p2_replica_det = verify_document_replicas(sample_phase2_ids)
    if not p2_replica_ok:
        print(f"ERROR: Phase 2 replica verification failed: {p2_replica_det}", file=sys.stderr)
        return EXIT_VERIFICATION_FAILURE
    print("Phase 2 replica verification: PASS (all sampled documents present on Node 0, Node 1, and Node 2).")

    # Wait for EventDispatcher bounded retries to exhaust
    print(f"Waiting {drain_buffer_s}s buffer for EventDispatcher bounded retries to exhaust...")
    time.sleep(drain_buffer_s)

    # Poll metrics until events_failed reflects all 50 failures or timeout
    exhaust_deadline = time.monotonic() + 10.0
    m_p2_drain: Dict[str, Any] = {}
    p2_failed_delta = 0
    p2_pub_delta = 0

    while time.monotonic() < exhaust_deadline:
        m_p2_drain = get_node_metrics(target_port)
        p2_failed_delta = (m_p2_drain.get("events_failed", 0) or 0) - baseline_failed
        p2_pub_delta = (m_p2_drain.get("events_published", 0) or 0) - baseline_published
        if p2_failed_delta == measured_count:
            break
        time.sleep(0.5)

    capture_metrics_snap("phase2_retries_exhausted")
    print(f"Phase 2 EventStore metrics: events_failed_delta={p2_failed_delta} (expected {measured_count}), events_published_delta={p2_pub_delta} (expected 0)")

    if p2_failed_delta != measured_count:
        print(f"ERROR: Expected exactly {measured_count} failed events delta, got {p2_failed_delta}!", file=sys.stderr)
        return EXIT_WORKLOAD_FAILURE

    if p2_pub_delta != 0:
        print(f"ERROR: False Kafka publication detected while Kafka was down! published_delta={p2_pub_delta}", file=sys.stderr)
        return EXIT_WORKLOAD_FAILURE

    # =========================================================================
    # PHASE 3 — Kafka Recovered (New Writes & Consumer Lag Recovery)
    # =========================================================================
    print("\n--- Phase 3: Kafka Recovered ---")
    print(f"Starting container '{KAFKA_CONTAINER}'...")
    start_ok, start_msg = docker_start_kafka(KAFKA_CONTAINER)
    if not start_ok:
        print(f"ERROR: Failed to start Kafka: {start_msg}", file=sys.stderr)
        return EXIT_INFRA_FAILURE

    print(f"Waiting for Kafka TCP reachability on {KAFKA_BOOTSTRAP_HOST}:{KAFKA_BOOTSTRAP_PORT} (timeout {kafka_timeout_s}s)...")
    k_ready, k_reach_time = wait_for_kafka_ready(timeout_s=kafka_timeout_s)
    if not k_ready:
        print(f"ERROR: Kafka did not become reachable on port {KAFKA_BOOTSTRAP_PORT} within {kafka_timeout_s}s", file=sys.stderr)
        return EXIT_INFRA_FAILURE
    print(f"Kafka reachable after {k_reach_time:.2f}s.")

    print("Waiting for consumer groups to re-register and recover...")
    cg_ready, cg_msg = wait_for_consumer_groups_ready(timeout_s=30.0)
    print(f"Consumer groups status: {cg_ready} ({cg_msg})")

    # Capture snapshot before issuing recovery writes
    snap_p3_pre = capture_metrics_snap("phase3_pre_recovery")
    m_p3_pre = get_node_metrics(target_port)
    p3_pre_failed = m_p3_pre.get("events_failed", 0) or 0
    p3_pre_published = m_p3_pre.get("events_published", 0) or 0

    print(f"Issuing {recovery_count} NEW sequential writes to Node 0...")
    phase3_latencies: List[float] = []
    phase3_failures: List[WriteRecord] = []
    p3_doc_ids: List[int] = []

    for i in range(recovery_count):
        req_id += 1
        doc_id = ID_RECOVERY_START + i
        p3_doc_ids.append(doc_id)
        status, lat_ms, err = send_write_request(target_port, doc_id, CONTENT_M)
        rec = WriteRecord(
            phase="phase3_recovery", request_id=req_id, doc_id=doc_id,
            http_status=status, success=(status == 201), latency_ms=lat_ms, error=err
        )
        all_writes.append(rec)
        if status == 201:
            phase3_latencies.append(lat_ms)
        else:
            phase3_failures.append(rec)

    phase3_success_cnt = len(phase3_latencies)
    phase3_fail_cnt = len(phase3_failures)
    print(f"Phase 3 writes completed: {phase3_success_cnt}/{recovery_count} HTTP 201 successes, {phase3_fail_cnt} failures.")

    if phase3_fail_cnt > 0:
        print(f"ERROR: Recovery writes failed: {phase3_fail_cnt} errors!", file=sys.stderr)
        return EXIT_WORKLOAD_FAILURE

    # Verify representative recovery documents on all 3 replicas
    sample_p3_ids = [p3_doc_ids[0], p3_doc_ids[len(p3_doc_ids) // 2], p3_doc_ids[-1]]
    print(f"Verifying Phase 3 representative documents on all 3 replicas: {sample_p3_ids}...")
    p3_replica_ok, p3_replica_det = verify_document_replicas(sample_p3_ids)
    if not p3_replica_ok:
        print(f"ERROR: Phase 3 replica verification failed: {p3_replica_det}", file=sys.stderr)
        return EXIT_VERIFICATION_FAILURE
    print("Phase 3 replica verification: PASS.")

    # Wait for recovery events to publish
    print(f"Waiting for recovery events to publish (polling Node 0 /metrics, timeout {recovery_drain_s}s)...")
    drain_deadline = time.monotonic() + recovery_drain_s
    recovery_pub_delta = 0
    recovery_failed_delta = 0
    m_final: Dict[str, Any] = {}

    while time.monotonic() < drain_deadline:
        m_final = get_node_metrics(target_port)
        recovery_pub_delta = (m_final.get("events_published", 0) or 0) - p3_pre_published
        recovery_failed_delta = (m_final.get("events_failed", 0) or 0) - p3_pre_failed
        if recovery_pub_delta >= recovery_count:
            break
        time.sleep(0.5)

    p3_drain_time = recovery_drain_s - max(0.0, drain_deadline - time.monotonic())
    capture_metrics_snap("phase3_post_drain")

    print(f"Phase 3 publication results: recovery_published_delta={recovery_pub_delta} (expected {recovery_count}), recovery_failed_delta={recovery_failed_delta} (expected 0)")

    # CRITICAL RECOVERY ASSERTION:
    # Do NOT require final events_failed == 0.
    # Expected final failed count is approximately: baseline_failed + 50.
    # The correct recovery assertion is: recovery_failed_delta == 0.
    expected_final_failed = compute_expected_final_failed(baseline_failed, measured_count)
    final_failed = m_final.get("events_failed", 0) or 0

    if recovery_failed_delta != 0:
        print(f"ERROR: recovery_failed_delta == {recovery_failed_delta} != 0 (NEW writes failed after Kafka recovery)!", file=sys.stderr)
        return EXIT_WORKLOAD_FAILURE

    if final_failed != expected_final_failed:
        print(f"ERROR: final_failed == {final_failed} != expected_final_failed ({expected_final_failed})!", file=sys.stderr)
        return EXIT_WORKLOAD_FAILURE

    # Verify Kafka consumer lag eventually returns to zero
    print("Waiting for Kafka consumer group lag to reach zero...")
    lag_deadline = time.monotonic() + recovery_drain_s
    final_lag: Optional[int] = None
    snap_final_lag: Optional[LagSnapshot] = None

    while time.monotonic() < lag_deadline:
        snap_final_lag = collect_lag_snapshot(
            sample_index=999, timestamp_rel_s=time.monotonic() - t_bench_start,
            phase="phase3_final_lag", cli_timeout_s=cli_timeout_s
        )
        if snap_final_lag.query_success:
            t_lag = snap_final_lag.total_lag()
            if t_lag is not None and t_lag == 0:
                final_lag = 0
                break
        time.sleep(1.0)

    if final_lag is None and snap_final_lag:
        final_lag = snap_final_lag.total_lag()

    print(f"Final Kafka consumer lag across all 3 groups: {final_lag}")
    lag_ok, lag_msg = evaluate_final_lag(final_lag, snap_final_lag)
    if not lag_ok:
        print(f"ERROR: Final consumer lag verification failed: {lag_msg}", file=sys.stderr)
        return EXIT_VERIFICATION_FAILURE
    print(f"Final consumer lag verification: PASS ({lag_msg}).")

    # Final document consistency verification across all 3 replicas
    all_sample_ids = sample_phase2_ids + sample_p3_ids
    print(f"\nVerifying final document consistency across all 3 replicas for all sample IDs {all_sample_ids}...")
    final_replica_ok, final_replica_det = verify_document_replicas(all_sample_ids)
    if not final_replica_ok:
        print(f"ERROR: Final document consistency verification failed: {final_replica_det}", file=sys.stderr)
        return EXIT_VERIFICATION_FAILURE
    print("Final document consistency: PASS (all sampled documents present on Node 0, Node 1, and Node 2).")

    # =========================================================================
    # Write CSV Results
    # =========================================================================
    # 1. Writes CSV
    with open(writes_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["phase", "request_id", "doc_id", "http_status", "success", "latency_ms", "error"])
        for w in all_writes:
            writer.writerow([w.phase, w.request_id, w.doc_id, w.http_status, w.success, round(w.latency_ms, 3), w.error])

    # 2. Metrics Timeline CSV
    with open(metrics_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "snapshot_id", "timestamp_rel_s", "phase",
            "events_total", "events_pending", "events_dispatching",
            "events_published", "events_failed", "events_retried", "events_replayed",
            "writes_total", "write_errors",
            "coordinator_writes_total", "coordinator_write_success", "coordinator_write_errors"
        ])
        for m in all_metrics:
            writer.writerow([
                m.snapshot_id, m.timestamp_rel_s, m.phase,
                m.events_total, m.events_pending, m.events_dispatching,
                m.events_published, m.events_failed, m.events_retried, m.events_replayed,
                m.writes_total, m.write_errors,
                m.coordinator_writes_total, m.coordinator_write_success, m.coordinator_write_errors
            ])

    # 3. Summary CSV
    p2_stats = compute_percentiles(phase2_latencies)
    p3_stats = compute_percentiles(phase3_latencies)
    overall_status = "PASS"

    with open(summary_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "run_id",
            "warmup_writes",
            "phase2_measured_writes",
            "phase3_recovery_writes",
            "phase2_http_success_count",
            "phase2_http_fail_count",
            "phase2_write_lat_mean_ms",
            "phase2_write_lat_p50_ms",
            "phase2_write_lat_p95_ms",
            "phase2_write_lat_p99_ms",
            "phase3_http_success_count",
            "phase3_http_fail_count",
            "phase3_write_lat_mean_ms",
            "phase2_replica_verification_pass",
            "phase3_replica_verification_pass",
            "final_replica_verification_pass",
            "baseline_events_failed",
            "phase2_events_failed_delta",
            "phase2_events_published_delta",
            "phase3_recovery_failed_delta",
            "phase3_recovery_published_delta",
            "expected_final_failed",
            "final_events_failed",
            "kafka_recovery_time_s",
            "phase3_drain_time_s",
            "phase3_final_lag",
            "status",
            "status_message"
        ])
        writer.writerow([
            run_id,
            warmup_count,
            measured_count,
            recovery_count,
            phase2_success_cnt,
            phase2_fail_cnt,
            p2_stats["mean"],
            p2_stats["p50"],
            p2_stats["p95"],
            p2_stats["p99"],
            phase3_success_cnt,
            phase3_fail_cnt,
            p3_stats["mean"],
            p2_replica_ok,
            p3_replica_ok,
            final_replica_ok,
            baseline_failed,
            p2_failed_delta,
            p2_pub_delta,
            recovery_failed_delta,
            recovery_pub_delta,
            expected_final_failed,
            final_failed,
            round(k_reach_time, 3),
            round(p3_drain_time, 3),
            final_lag if final_lag is not None else -1,
            overall_status,
            "Resilience validated: synchronous writes succeeded during Kafka outage, bounded failure recorded, new writes published after recovery"
        ])

    print("\n================================================================")
    print("Benchmark I: SUCCESS (PASS)")
    print(f"  Phase 2 Writes (Kafka down): {phase2_success_cnt}/{measured_count} (HTTP 201)")
    print(f"  Phase 2 Failure Delta:       {p2_failed_delta} (Expected {measured_count})")
    print(f"  Phase 3 Recovery Writes:     {phase3_success_cnt}/{recovery_count} (HTTP 201)")
    print(f"  Phase 3 New Failures Delta:  {recovery_failed_delta} (Expected 0)")
    print(f"  Final Events Failed:         {final_failed} (Expected {expected_final_failed})")
    print(f"  Final Consumer Lag:          {final_lag}")
    print("================================================================")
    return EXIT_PASS


# ---------------------------------------------------------------------------
# Harness Self-Test Suite (Hermetic, Zero Dependencies)
# ---------------------------------------------------------------------------

def run_self_tests() -> int:
    """Hermetic unit tests validating Benchmark I calculation, parsing, and failure logic."""
    print("Running Benchmark I harness self-tests...")

    # Test 1: Metric delta calculations
    before_m = {
        "events_total": 10, "events_pending": 0, "events_dispatching": 0,
        "events_published": 10, "events_failed": 2, "events_retried": 0, "events_replayed": 0,
        "writes_total": 10, "write_errors": 0,
        "coordinator_writes_total": 10, "coordinator_write_success": 10, "coordinator_write_errors": 0
    }
    after_m = {
        "events_total": 60, "events_pending": 0, "events_dispatching": 0,
        "events_published": 10, "events_failed": 52, "events_retried": 150, "events_replayed": 0,
        "writes_total": 60, "write_errors": 0,
        "coordinator_writes_total": 60, "coordinator_write_success": 60, "coordinator_write_errors": 0
    }
    deltas = compute_metric_deltas(before_m, after_m)
    assert deltas["events_failed_delta"] == 50, f"Expected failed_delta=50, got {deltas['events_failed_delta']}"
    assert deltas["events_published_delta"] == 0, f"Expected published_delta=0, got {deltas['events_published_delta']}"
    assert deltas["events_retried_delta"] == 150, f"Expected retried_delta=150, got {deltas['events_retried_delta']}"
    assert deltas["writes_total_delta"] == 50, f"Expected writes_total_delta=50, got {deltas['writes_total_delta']}"
    print("  [PASS] Test 1: Metric delta calculations")

    # Test 2: recovery_failed_delta calculation (zero new failures)
    p3_pre = {"events_failed": 52, "events_published": 10}
    p3_post = {"events_failed": 52, "events_published": 20}
    rf_delta = compute_recovery_failed_delta(p3_pre, p3_post)
    assert rf_delta == 0, f"Expected recovery_failed_delta=0, got {rf_delta}"
    print("  [PASS] Test 2: recovery_failed_delta calculation (zero new failures)")

    # Test 3: recovery_failed_delta non-zero detection
    p3_post_bad = {"events_failed": 53, "events_published": 19}
    rf_delta_bad = compute_recovery_failed_delta(p3_pre, p3_post_bad)
    assert rf_delta_bad == 1, f"Expected recovery_failed_delta=1, got {rf_delta_bad}"
    print("  [PASS] Test 3: recovery_failed_delta non-zero detection")

    # Test 4: Expected final failed count logic
    exp_final = compute_expected_final_failed(baseline_failed=2, phase2_write_count=50)
    assert exp_final == 52, f"Expected final failed count 52, got {exp_final}"
    # Verify that asserting final_failed == 0 would be WRONG
    assert exp_final != 0, "final_failed == 0 assertion must NOT be required"
    print("  [PASS] Test 4: Expected final failed count logic")

    # Test 5: HTTP response parsing / WriteRecord construction
    wr_ok = WriteRecord("phase2", 1, 9_001_001, 201, True, 12.3, "")
    assert wr_ok.success is True and wr_ok.http_status == 201
    wr_err = WriteRecord("phase2", 2, 9_001_002, 500, False, 15.0, "Internal error")
    assert wr_err.success is False and wr_err.http_status == 500
    print("  [PASS] Test 5: HTTP response parsing / WriteRecord construction")

    # Test 6: Kafka CLI failure handling (Never convert failure to lag=0)
    def mock_failing_query(g: str) -> Tuple[List[PartitionLagRecord], bool, str]:
        recs = [
            PartitionLagRecord(group=g, partition=p, current_offset=None, log_end_offset=None, lag=None, query_failed=True, error_msg="Timeout")
            for p in PARTITIONS
        ]
        return recs, False, "Timeout"

    snap_fail = collect_lag_snapshot(1, 1.0, "phase2", query_fn=mock_failing_query)
    assert snap_fail.query_success is False, "Failing snapshot must have query_success=False"
    assert snap_fail.total_lag() is None, "Failing snapshot total_lag must be None, NEVER 0"
    for r in snap_fail.records:
        assert r.query_failed is True
        assert r.lag is None, "Record lag must be None, never 0"
    print("  [PASS] Test 6: Kafka CLI failure handling (never converts to lag=0)")

    # Test 7: Kafka lag calculation (standard and uncommitted '-')
    sample_describe_output = """
GROUP           TOPIC               PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
dse-node-0      documents.mutations 0          100             105             5               dse-node-0-1    /127.0.0.1      dse-node-0-consumer
dse-node-0      documents.mutations 1          -               12              -               dse-node-0-1    /127.0.0.1      dse-node-0-consumer
dse-node-0      documents.mutations 2          50              50              0               dse-node-0-1    /127.0.0.1      dse-node-0-consumer
"""
    recs, success, err = parse_consumer_group_describe_output("dse-node-0", sample_describe_output)
    assert success is True, f"Parsing failed: {err}"
    assert len(recs) == 3
    # Partition 0: lag=5
    assert recs[0].partition == 0 and recs[0].current_offset == 100 and recs[0].log_end_offset == 105 and recs[0].lag == 5
    # Partition 1: uncommitted '-', lag = LOG-END-OFFSET = 12
    assert recs[1].partition == 1 and recs[1].current_offset is None and recs[1].log_end_offset == 12 and recs[1].lag == 12
    # Partition 2: lag=0
    assert recs[2].partition == 2 and recs[2].current_offset == 50 and recs[2].log_end_offset == 50 and recs[2].lag == 0
    print("  [PASS] Test 7: Kafka lag calculation and uncommitted '-' handling")

    # Test 8: Result freshness / overwrite protection
    test_existing = [__file__]  # This file definitely exists
    try:
        check_result_freshness(test_existing, allow_overwrite=False)
        assert False, "check_result_freshness should have raised FileExistsError"
    except FileExistsError:
        pass  # Expected
    # Non-existent files should pass
    check_result_freshness(["/path/to/nonexistent/phase21_fake_summary.csv"], allow_overwrite=False)
    print("  [PASS] Test 8: Result freshness / overwrite protection")

    # Test 9: Percentile statistics calculations
    stats = compute_percentiles([10.0, 20.0, 30.0, 40.0, 50.0])
    assert stats["min"] == 10.0
    assert stats["max"] == 50.0
    assert stats["mean"] == 30.0
    assert stats["p50"] == 30.0
    print("  [PASS] Test 9: Percentile statistics calculations")

    # Test 10: Final lag decision logic (harden lag assertion)
    # 10a: successful total_lag == 0 => pass
    snap_zero = LagSnapshot(
        sample_index=1, timestamp_rel_s=1.0, phase="test",
        records=[
            PartitionLagRecord("dse-node-0", 0, 100, 100, 0, False, ""),
            PartitionLagRecord("dse-node-0", 1, 100, 100, 0, False, ""),
            PartitionLagRecord("dse-node-0", 2, 100, 100, 0, False, ""),
            PartitionLagRecord("dse-node-1", 0, 100, 100, 0, False, ""),
            PartitionLagRecord("dse-node-1", 1, 100, 100, 0, False, ""),
            PartitionLagRecord("dse-node-1", 2, 100, 100, 0, False, ""),
            PartitionLagRecord("dse-node-2", 0, 100, 100, 0, False, ""),
            PartitionLagRecord("dse-node-2", 1, 100, 100, 0, False, ""),
            PartitionLagRecord("dse-node-2", 2, 100, 100, 0, False, ""),
        ],
        query_success=True,
        error_msg=""
    )
    assert snap_zero.total_lag() == 0
    ok, msg = evaluate_final_lag(0, snap_zero)
    assert ok is True, f"total_lag == 0 must pass, got: {msg}"

    # 10b: successful total_lag > 0 => fail
    snap_nonzero = LagSnapshot(
        sample_index=2, timestamp_rel_s=2.0, phase="test",
        records=[
            PartitionLagRecord("dse-node-0", 0, 95, 100, 5, False, ""),
            PartitionLagRecord("dse-node-0", 1, 100, 100, 0, False, ""),
            PartitionLagRecord("dse-node-0", 2, 100, 100, 0, False, ""),
            PartitionLagRecord("dse-node-1", 0, 100, 100, 0, False, ""),
            PartitionLagRecord("dse-node-1", 1, 100, 100, 0, False, ""),
            PartitionLagRecord("dse-node-1", 2, 100, 100, 0, False, ""),
            PartitionLagRecord("dse-node-2", 0, 100, 100, 0, False, ""),
            PartitionLagRecord("dse-node-2", 1, 100, 100, 0, False, ""),
            PartitionLagRecord("dse-node-2", 2, 100, 100, 0, False, ""),
        ],
        query_success=True,
        error_msg=""
    )
    assert snap_nonzero.total_lag() == 5
    ok, msg = evaluate_final_lag(5, snap_nonzero)
    assert ok is False, "total_lag > 0 must fail"
    assert "did not reach zero" in msg

    # 10c: query failure / total_lag None => fail
    snap_failed_query = LagSnapshot(
        sample_index=3, timestamp_rel_s=3.0, phase="test",
        records=[
            PartitionLagRecord("dse-node-0", 0, None, None, None, True, "Timeout"),
            PartitionLagRecord("dse-node-0", 1, None, None, None, True, "Timeout"),
            PartitionLagRecord("dse-node-0", 2, None, None, None, True, "Timeout"),
            PartitionLagRecord("dse-node-1", 0, None, None, None, True, "Timeout"),
            PartitionLagRecord("dse-node-1", 1, None, None, None, True, "Timeout"),
            PartitionLagRecord("dse-node-1", 2, None, None, None, True, "Timeout"),
            PartitionLagRecord("dse-node-2", 0, None, None, None, True, "Timeout"),
            PartitionLagRecord("dse-node-2", 1, None, None, None, True, "Timeout"),
            PartitionLagRecord("dse-node-2", 2, None, None, None, True, "Timeout"),
        ],
        query_success=False,
        error_msg="Command timed out"
    )
    assert snap_failed_query.total_lag() is None
    ok, msg = evaluate_final_lag(None, snap_failed_query)
    assert ok is False, "query failure / total_lag None must fail"

    # 10d: query failure with false 0 => fail (never interpret query failure as 0)
    ok, msg = evaluate_final_lag(0, snap_failed_query)
    assert ok is False, "query failure must fail even if 0 passed"

    # 10e: no snapshot / None lag => fail
    ok, msg = evaluate_final_lag(None, None)
    assert ok is False, "None lag without snapshot must fail"
    print("  [PASS] Test 10: Final lag decision logic (harden lag assertion)")

    print("\nAll 10 Benchmark I harness self-tests PASSED successfully.")
    return EXIT_PASS


# ---------------------------------------------------------------------------
# CLI Argument Parser
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 21 Benchmark I: Kafka Unavailability & Write-Path Resilience")
    parser.add_argument("--self-test", action="store_true", help="Run hermetic harness self-tests and exit")
    parser.add_argument("--run-id", type=str, default=DEFAULT_RUN_ID, help="Benchmark run identifier")
    parser.add_argument("--results-dir", type=str, default=DEFAULT_RESULTS_DIR, help="Results output directory")
    parser.add_argument("--out-prefix", type=str, default=None, help="Output CSV path prefix (optional)")
    parser.add_argument("--target-port", type=int, default=DEFAULT_TARGET_PORT, help="Node 0 HTTP port")
    parser.add_argument("--warmup", type=int, default=DEFAULT_WARMUP_WRITES, help="Warmup write count")
    parser.add_argument("--measured", type=int, default=DEFAULT_MEASURED_WRITES, help="Phase 2 measured write count")
    parser.add_argument("--recovery", type=int, default=DEFAULT_RECOVERY_WRITES, help="Phase 3 recovery write count")
    parser.add_argument("--drain-buffer", type=float, default=DEFAULT_DRAIN_BUFFER_SEC, help="Phase 2 retry drain buffer (sec)")
    parser.add_argument("--kafka-timeout", type=float, default=DEFAULT_KAFKA_TIMEOUT_SEC, help="Kafka restart reachability timeout (sec)")
    parser.add_argument("--recovery-drain", type=float, default=DEFAULT_RECOVERY_DRAIN_SEC, help="Phase 3 publication/drain timeout (sec)")
    parser.add_argument("--cli-timeout", type=float, default=DEFAULT_KAFKA_CLI_TIMEOUT_SEC, help="Kafka CLI per-query timeout (sec)")

    args = parser.parse_args()

    if args.self_test:
        return run_self_tests()

    return run_benchmark_i(
        target_port=args.target_port,
        warmup_count=args.warmup,
        measured_count=args.measured,
        recovery_count=args.recovery,
        drain_buffer_s=args.drain_buffer,
        kafka_timeout_s=args.kafka_timeout,
        recovery_drain_s=args.recovery_drain,
        cli_timeout_s=args.cli_timeout,
        results_dir=args.results_dir,
        run_id=args.run_id,
        out_prefix=args.out_prefix
    )


if __name__ == "__main__":
    sys.exit(main())
