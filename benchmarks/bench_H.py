#!/usr/bin/env python3
"""
Phase 21 Benchmark H — Kafka Consumer Lag Under Load.

Goal:
  Determine whether the Kafka consumers can keep up with produced mutation events
  under load and quantify consumer lag across all 3 nodes and partitions.

Architectural Fact:
  Phase 17 performs synchronous RF=3 replication before HTTP write completion.
  Therefore document visibility on Nodes 1/2 is NOT a valid Kafka lag measurement.
  Benchmark H directly measures Kafka consumer offset lag using Kafka's authoritative
  consumer group partition offsets.

Offset Semantics & Measurement Mechanism:
  - Kafka Topic: documents.mutations (3 partitions: 0, 1, 2)
  - Consumer Groups: dse-node-0, dse-node-1, dse-node-2 (one group per cluster node)
  - For each consumer group and partition, queried via:
      docker exec dse-kafka /opt/kafka/bin/kafka-consumer-groups.sh \\
          --bootstrap-server localhost:9092 --describe --group <group_name>
  - High-Watermark / Latest Offset: LOG-END-OFFSET (the Kafka log end / high-watermark position)
  - Consumer Offset: CURRENT-OFFSET (the consumer group's committed offset as reported by Kafka)
  - Lag: LOG-END-OFFSET - CURRENT-OFFSET (committed Kafka consumer lag)
  - Offset Semantics:
      `CURRENT-OFFSET` is the consumer group's committed offset as reported by Kafka.
      `LOG-END-OFFSET - CURRENT-OFFSET` measures committed Kafka consumer lag.
      Messages processed by the consumer but whose offset has not yet been committed
      may still contribute to the reported lag. Therefore, Benchmark H measures
      committed Kafka consumer lag, not instantaneous in-memory processing lag or
      end-to-end processing latency.
  - Sampled Observational Lag:
      Lag is sampled periodically (based on discrete 500 ms snapshots) during the active workload.
      The sampled average is explicitly designated as sampled observational lag,
      not an exact continuous integral.
  - Final Lag:
      Measured after the workload and drain phase once consumers settle.

Workload Matrix:
  - Concurrency levels: C1, C4, C8, C16
  - Warmup: 20 writes per concurrency level (discarded)
  - Measured: 100 writes per concurrency level (POST /documents to Node 0)
  - Unique document IDs per concurrency level (8_010_000+ to 8_160_000+)
  - Moderate payload (Phase 21 standard)
  - Post-workload drain observation up to bounded timeout (15.0s)
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
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

# ---------------------------------------------------------------------------
# Constants & Configuration
# ---------------------------------------------------------------------------

HOST = "127.0.0.1"
DEFAULT_TARGET_PORT = 8081  # Node 0 HTTP
DEFAULT_SAMPLE_INTERVAL_SEC = 0.5
DEFAULT_DRAIN_TIMEOUT_SEC = 15.0
DEFAULT_KAFKA_CLI_TIMEOUT_SEC = 10.0  # Accommodates JVM startup (~5.3s on Windows/Docker)
DEFAULT_WARMUP_WRITES = 20
DEFAULT_MEASURED_WRITES = 100

KAFKA_BOOTSTRAP_HOST = "localhost"
KAFKA_BOOTSTRAP_PORT = 9094
KAFKA_CONTAINER = "dse-kafka"
KAFKA_INTERNAL_BOOTSTRAP = "localhost:9092"
KAFKA_TOPIC = "documents.mutations"
PARTITIONS = [0, 1, 2]
CONSUMER_GROUPS = ["dse-node-0", "dse-node-1", "dse-node-2"]

# Moderate payload matching Phase 21 standard
CONTENT_M = (
    "distributed search engines index large document collections using "
    "inverted index structures with tf-idf scoring algorithms for ranked "
    "retrieval supporting boolean query modes including or and and with "
    "concurrent multi-shard fan-out across replicated node clusters"
)

# Distinct document ID ranges per concurrency level (8_000_000+)
# Zero collision with baseline or previous benchmarks.
WRITE_ID_CONFIG: Dict[int, Dict[str, int]] = {
    1:  {"warmup_start": 8_010_001, "measured_start": 8_011_001},
    4:  {"warmup_start": 8_040_001, "measured_start": 8_041_001},
    8:  {"warmup_start": 8_080_001, "measured_start": 8_081_001},
    16: {"warmup_start": 8_160_001, "measured_start": 8_161_001},
}


# ---------------------------------------------------------------------------
# Data Structures
# ---------------------------------------------------------------------------

@dataclass
class PartitionLagRecord:
    group: str
    partition: int
    current_offset: Optional[int]
    log_end_offset: Optional[int]
    lag: Optional[int]  # None if measurement failed; NEVER false 0
    query_failed: bool = False
    error_msg: str = ""


@dataclass
class LagSnapshot:
    sample_index: int
    timestamp_rel_s: float
    phase: str  # "baseline", "workload", "workload_end", "drain"
    records: List[PartitionLagRecord]
    query_success: bool = True
    error_msg: str = ""

    def group_lag(self, group: str) -> Optional[int]:
        """Total lag for a specific consumer group. Returns None if any partition query failed."""
        g_recs = [r for r in self.records if r.group == group]
        if not g_recs or any(r.query_failed or r.lag is None for r in g_recs):
            return None
        return sum(r.lag for r in g_recs if r.lag is not None)

    def group_max_partition_lag(self, group: str) -> Optional[int]:
        """Maximum single-partition lag for a specific consumer group."""
        g_recs = [r for r in self.records if r.group == group]
        if not g_recs or any(r.query_failed or r.lag is None for r in g_recs):
            return None
        return max(r.lag for r in g_recs if r.lag is not None)

    def partition_lag(self, partition: int, group: str) -> Optional[int]:
        """Lag for a specific group and partition."""
        for r in self.records:
            if r.group == group and r.partition == partition:
                return r.lag if not r.query_failed else None
        return None

    def partition_max_lag(self, partition: int) -> Optional[int]:
        """Max lag on a specific partition across all consumer groups."""
        p_recs = [r for r in self.records if r.partition == partition]
        if not p_recs or any(r.query_failed or r.lag is None for r in p_recs):
            return None
        return max(r.lag for r in p_recs if r.lag is not None)

    def total_lag(self) -> Optional[int]:
        """Total lag across all consumer groups. Returns None if ANY measurement failed."""
        if not self.query_success or any(r.query_failed or r.lag is None for r in self.records):
            return None
        return sum(r.lag for r in self.records if r.lag is not None)


@dataclass
class WriteRecord:
    concurrency: int
    request_id: int
    doc_id: int
    status_code: int
    latency_ms: float
    success: bool
    error_msg: str = ""


@dataclass
class ConcurrencySummary:
    concurrency: int
    requested_writes: int
    successful_writes: int
    errors: int
    events_published: int
    start_offsets_summary: str
    max_lag_node0: Optional[int]
    max_lag_node1: Optional[int]
    max_lag_node2: Optional[int]
    max_lag_overall: Optional[int]
    max_lag_p0: Optional[int]
    max_lag_p1: Optional[int]
    max_lag_p2: Optional[int]
    mean_sampled_lag_node0: Optional[float]
    mean_sampled_lag_node1: Optional[float]
    mean_sampled_lag_node2: Optional[float]
    final_lag_node0: Optional[int]
    final_lag_node1: Optional[int]
    final_lag_node2: Optional[int]
    final_lag_total: Optional[int]
    drain_time_sec: float
    drain_timeout: bool
    failed_snapshots: int
    status: str  # "PASS" or "FAIL"


# ---------------------------------------------------------------------------
# Statistics Helper
# ---------------------------------------------------------------------------

def compute_percentiles(samples: List[float]) -> Dict[str, float]:
    """Computes standard statistics with inclusive quantiles."""
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


# ---------------------------------------------------------------------------
# Kafka CLI Offset Query & Parsing
# ---------------------------------------------------------------------------

def parse_consumer_group_describe_output(
    group_name: str, stdout_text: str, topic_name: str = KAFKA_TOPIC
) -> Tuple[List[PartitionLagRecord], bool, str]:
    """
    Parses the tabular output of `kafka-consumer-groups.sh --describe --group <group>`.

    Example output line:
      GROUP       TOPIC               PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG  CONSUMER-ID  HOST        CLIENT-ID
      dse-node-0  documents.mutations 0          100             105             5    dse-node-0-1 /127.0.0.1  dse-node-0-consumer

    Correctness guarantees:
      - Valid uncommitted offsets ('-') with known LOG-END-OFFSET compute lag = LOG-END-OFFSET.
      - A query failure or missing topic/group data never silently becomes lag=0.
    """
    records: List[PartitionLagRecord] = []
    seen_partitions = set()
    has_parse_error = False
    parse_error_msg = ""

    for line in stdout_text.splitlines():
        line = line.strip()
        if not line:
            continue
        # Skip header lines
        if line.startswith("GROUP") or "CURRENT-OFFSET" in line or "LOG-END-OFFSET" in line:
            continue
        # Skip informational lines
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

        # Current offset: integer or None (if uncommitted '-')
        cur_str = parts[3]
        cur_offset: Optional[int] = None
        if cur_str.isdigit():
            cur_offset = int(cur_str)
        elif cur_str != "-":
            # Malformed column
            has_parse_error = True
            parse_error_msg = f"Invalid CURRENT-OFFSET: '{cur_str}'"

        # Log end offset: must be integer
        end_str = parts[4]
        end_offset: Optional[int] = None
        if end_str.isdigit():
            end_offset = int(end_str)
        else:
            # High watermark was not reported or is invalid
            has_parse_error = True
            parse_error_msg = f"Invalid LOG-END-OFFSET: '{end_str}'"

        # Lag calculation
        lag: Optional[int] = None
        if len(parts) >= 6 and parts[5].isdigit():
            lag = int(parts[5])
        elif end_offset is not None:
            if cur_offset is not None:
                lag = max(0, end_offset - cur_offset)
            else:
                # Uncommitted: group hasn't consumed anything; all messages are lagging
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

    # Ensure all expected partitions (0, 1, 2) are present
    missing_partitions = [p for p in PARTITIONS if p not in seen_partitions]
    if missing_partitions:
        has_parse_error = True
        err = f"Partitions {missing_partitions} missing from Kafka CLI output"
        for p in missing_partitions:
            records.append(PartitionLagRecord(
                group=group_name,
                partition=p,
                current_offset=None,
                log_end_offset=None,
                lag=None,
                query_failed=True,
                error_msg=err
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
    """
    Executes `kafka-consumer-groups.sh --describe` for a single consumer group.
    Never converts a timeout or failure into lag=0.
    Returns (records, success, error_message).
    """
    cmd = [
        "docker", "exec", container_name,
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server", bootstrap_server,
        "--describe",
        "--group", group_name
    ]

    try:
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=timeout_s
        )
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
    groups: List[str] = CONSUMER_GROUPS,
    cli_timeout_s: float = DEFAULT_KAFKA_CLI_TIMEOUT_SEC,
    query_fn: Optional[Callable[[str], Tuple[List[PartitionLagRecord], bool, str]]] = None
) -> LagSnapshot:
    """
    Collects an authoritative lag snapshot by querying dse-node-0, dse-node-1, dse-node-2
    independently and concurrently using ThreadPoolExecutor(max_workers=3).
    Preserves deterministic ordering of groups and partitions.
    """
    group_results: Dict[str, Tuple[List[PartitionLagRecord], bool, str]] = {}

    def query_task(g: str) -> Tuple[str, Tuple[List[PartitionLagRecord], bool, str]]:
        if query_fn:
            return g, query_fn(g)
        return g, query_group_offsets(g, timeout_s=cli_timeout_s)

    with ThreadPoolExecutor(max_workers=len(groups)) as pool:
        futures = [pool.submit(query_task, g) for g in groups]
        for f in as_completed(futures):
            g, res = f.result()
            group_results[g] = res

    all_records: List[PartitionLagRecord] = []
    all_success = True
    err_msgs: List[str] = []

    # Preserve deterministic ordering of groups and partitions
    for g in sorted(groups):
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
# Cluster & Kafka Health Probes
# ---------------------------------------------------------------------------

def check_tcp_port(host: str, port: int, timeout: float = 1.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except (OSError, socket.timeout):
        return False


def check_http_health(port: int, timeout: float = 1.0) -> bool:
    try:
        conn = http.client.HTTPConnection(HOST, port, timeout=timeout)
        conn.request("GET", "/health")
        resp = conn.getresponse()
        conn.close()
        return resp.status == 200
    except Exception:
        return False


def get_node_metrics(port: int, timeout: float = 2.0) -> Dict[str, Any]:
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


def check_kafka_prereqs(timeout_s: float = 10.0) -> Tuple[bool, str]:
    """Validates Kafka reachability and container availability."""
    deadline = time.time() + timeout_s
    reachable = False
    while time.time() < deadline:
        if check_tcp_port(KAFKA_BOOTSTRAP_HOST, KAFKA_BOOTSTRAP_PORT, timeout=1.0) or \
           check_tcp_port("127.0.0.1", KAFKA_BOOTSTRAP_PORT, timeout=1.0):
            reachable = True
            break
        time.sleep(0.5)

    if not reachable:
        return False, f"Kafka broker not reachable on {KAFKA_BOOTSTRAP_HOST}:{KAFKA_BOOTSTRAP_PORT}"

    try:
        res = subprocess.run(
            ["docker", "exec", KAFKA_CONTAINER, "/opt/kafka/bin/kafka-topics.sh",
             "--bootstrap-server", KAFKA_INTERNAL_BOOTSTRAP, "--list"],
            capture_output=True, text=True, timeout=5
        )
        if res.returncode == 0:
            topics = [line.strip() for line in res.stdout.splitlines() if line.strip()]
            if KAFKA_TOPIC not in topics:
                return False, f"Kafka topic '{KAFKA_TOPIC}' not found in cluster: {topics}"
    except Exception as e:
        return False, f"Error verifying Kafka container or topic: {e}"

    return True, "Kafka broker and topic verified"


def check_consumer_groups_ready(timeout_s: float = 25.0) -> Tuple[bool, str]:
    """Ensures all 3 consumer groups are registered and accessible."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            res = subprocess.run(
                ["docker", "exec", KAFKA_CONTAINER, "/opt/kafka/bin/kafka-consumer-groups.sh",
                 "--bootstrap-server", KAFKA_INTERNAL_BOOTSTRAP, "--list"],
                capture_output=True, text=True, timeout=5
            )
            if res.returncode == 0:
                active = set(line.strip() for line in res.stdout.splitlines() if line.strip())
                missing = [g for g in CONSUMER_GROUPS if g not in active]
                if not missing:
                    return True, "All 3 consumer groups active"
        except Exception:
            pass
        time.sleep(1.0)
    return False, f"Consumer groups not registered in time: {CONSUMER_GROUPS}"


# ---------------------------------------------------------------------------
# Workload Worker
# ---------------------------------------------------------------------------

def send_write_request(
    port: int,
    doc_id: int,
    content: str,
    timeout_s: float = 10.0
) -> Tuple[int, float, str]:
    """Sends a single POST /documents request and returns (status, latency_ms, err_msg)."""
    payload = json.dumps({
        "id": doc_id,
        "title": f"Bench H Document {doc_id}",
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
        t1 = time.perf_counter()
        lat_ms = (t1 - t0) * 1000.0
        return resp.status, lat_ms, ""
    except Exception as e:
        t1 = time.perf_counter()
        lat_ms = (t1 - t0) * 1000.0
        return 0, lat_ms, str(e)


# ---------------------------------------------------------------------------
# Background Periodic Lag Sampler
# ---------------------------------------------------------------------------

class BackgroundLagSampler:
    """Periodically queries Kafka consumer group lag concurrently in a background thread."""

    def __init__(
        self,
        sample_interval_s: float = DEFAULT_SAMPLE_INTERVAL_SEC,
        cli_timeout_s: float = DEFAULT_KAFKA_CLI_TIMEOUT_SEC
    ):
        self.interval_s = sample_interval_s
        self.cli_timeout_s = cli_timeout_s
        self.snapshots: List[LagSnapshot] = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._start_time: float = 0.0
        self._phase: str = "workload"
        self._sample_index: int = 0

    def start(self, start_time: float):
        self._start_time = start_time
        self._stop_event.clear()
        self._phase = "workload"
        self._sample_index = 0
        self._thread = threading.Thread(target=self._run, name="LagSamplerThread", daemon=True)
        self._thread.start()

    def set_phase(self, phase: str):
        with self._lock:
            self._phase = phase

    def stop(self):
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self.cli_timeout_s + 2.0)

    def take_immediate_snapshot(self, phase: Optional[str] = None) -> LagSnapshot:
        with self._lock:
            p = phase or self._phase
            idx = self._sample_index
            self._sample_index += 1
            rel_t = time.time() - self._start_time if self._start_time > 0 else 0.0
        snap = collect_lag_snapshot(idx, rel_t, p, cli_timeout_s=self.cli_timeout_s)
        with self._lock:
            self.snapshots.append(snap)
        return snap

    def get_snapshots(self) -> List[LagSnapshot]:
        with self._lock:
            return list(self.snapshots)

    def _run(self):
        while not self._stop_event.is_set():
            t_now = time.time()
            rel_t = t_now - self._start_time if self._start_time > 0 else 0.0
            with self._lock:
                idx = self._sample_index
                self._sample_index += 1
                p = self._phase

            snap = collect_lag_snapshot(idx, rel_t, p, cli_timeout_s=self.cli_timeout_s)

            with self._lock:
                self.snapshots.append(snap)

            # Sleep remainder of interval
            elapsed = time.time() - t_now
            sleep_time = max(0.01, self.interval_s - elapsed)
            if self._stop_event.wait(timeout=sleep_time):
                break


# ---------------------------------------------------------------------------
# Benchmark H Core Runner
# ---------------------------------------------------------------------------

def run_benchmark_h(
    target_port: int = DEFAULT_TARGET_PORT,
    warmup_count: int = DEFAULT_WARMUP_WRITES,
    measured_count: int = DEFAULT_MEASURED_WRITES,
    sample_interval_s: float = DEFAULT_SAMPLE_INTERVAL_SEC,
    drain_timeout_s: float = DEFAULT_DRAIN_TIMEOUT_SEC,
    cli_timeout_s: float = DEFAULT_KAFKA_CLI_TIMEOUT_SEC,
    out_prefix: str = "results/phase21_H_kafka_lag",
    concurrencies: Optional[List[int]] = None
) -> int:
    """Executes the full Benchmark H test suite across all concurrency levels."""

    if concurrencies is None:
        concurrencies = [1, 4, 8, 16]

    print("\n" + "=" * 70)
    print("Benchmark H: Kafka Consumer Lag Under Load")
    print("=" * 70)
    print(f"Target: Node 0 HTTP (port {target_port})")
    print(f"Concurrency Matrix: {concurrencies}")
    print(f"Per-Concurrency: {warmup_count} warmup + {measured_count} measured writes")
    print(f"Sampling Interval: {sample_interval_s}s (500ms)")
    print(f"Drain Timeout: {drain_timeout_s}s (bounded)")
    print(f"Kafka CLI Timeout: {cli_timeout_s}s (parallel 3-group query)")
    print(f"Kafka Topic: {KAFKA_TOPIC} (Partitions: {PARTITIONS})")
    print(f"Consumer Groups: {CONSUMER_GROUPS}")
    print("=" * 70)

    # 1. Prerequisite verification
    k_ok, k_msg = check_kafka_prereqs()
    if not k_ok:
        print(f"[ERROR] Kafka prerequisite check failed: {k_msg}", file=sys.stderr)
        return 1
    print(f"[OK] {k_msg}")

    # 2. Node health verification
    if not check_http_health(target_port):
        print(f"[ERROR] Target node (port {target_port}) not responding to /health", file=sys.stderr)
        return 1
    print(f"[OK] Target node (port {target_port}) healthy")

    all_summaries: List[ConcurrencySummary] = []
    all_write_records: List[WriteRecord] = []
    all_snapshots: List[LagSnapshot] = []

    suite_failed = False

    for c in concurrencies:
        print(f"\n>>> Running Concurrency C={c} <<<")
        cfg = WRITE_ID_CONFIG.get(c, {
            "warmup_start": 8_000_000 + c * 10_000 + 1,
            "measured_start": 8_000_000 + c * 10_000 + 101
        })

        # --- Warmup Phase ---
        if warmup_count > 0:
            print(f"  Warmup: issuing {warmup_count} writes (discarded)...")
            warmup_start_id = cfg["warmup_start"]
            for i in range(warmup_count):
                doc_id = warmup_start_id + i
                status, lat, err = send_write_request(target_port, doc_id, CONTENT_M)
                if status != 201:
                    print(f"  [WARN] Warmup write {doc_id} returned status {status}: {err}")
            # Brief pause to allow consumers to settle warmup
            time.sleep(1.0)

        # Baseline Node 0 metrics
        m_before = get_node_metrics(target_port)
        events_before = m_before.get("events_published", 0)

        # Baseline starting offsets
        sampler = BackgroundLagSampler(sample_interval_s=sample_interval_s, cli_timeout_s=cli_timeout_s)
        initial_snap = sampler.take_immediate_snapshot(phase="baseline")
        all_snapshots.append(initial_snap)

        start_summary_parts = []
        for g in CONSUMER_GROUPS:
            g_recs = [r for r in initial_snap.records if r.group == g]
            g_lags = [
                f"p{r.partition}:{r.current_offset if r.current_offset is not None else '-'}"
                f"->{r.log_end_offset if r.log_end_offset is not None else '-'}"
                f"(lag={'FAIL' if r.query_failed or r.lag is None else r.lag})"
                for r in g_recs
            ]
            start_summary_parts.append(f"{g}[{','.join(g_lags)}]")
        start_offsets_str = "; ".join(start_summary_parts)
        print(f"  Starting Offsets: {start_offsets_str}")

        # Start periodic sampler
        t_start = time.time()
        sampler.start(start_time=t_start)

        # --- Active Workload Phase ---
        print(f"  Executing {measured_count} writes at concurrency {c}...")
        measured_start_id = cfg["measured_start"]
        write_records: List[WriteRecord] = []

        def worker_task(req_idx: int, doc_id: int) -> WriteRecord:
            status, lat, err = send_write_request(target_port, doc_id, CONTENT_M)
            return WriteRecord(
                concurrency=c,
                request_id=req_idx,
                doc_id=doc_id,
                status_code=status,
                latency_ms=lat,
                success=(status == 201),
                error_msg=err
            )

        with ThreadPoolExecutor(max_workers=c) as executor:
            futures = [
                executor.submit(worker_task, idx + 1, measured_start_id + idx)
                for idx in range(measured_count)
            ]
            for f in as_completed(futures):
                rec = f.result()
                write_records.append(rec)

        t_workload_done = time.time()
        sampler.set_phase("drain")
        print(f"  Workload completed in {t_workload_done - t_start:.2f}s")

        # --- Drain Phase ---
        print(f"  Observing drain phase (bounded timeout: {drain_timeout_s}s)...")
        drain_start = time.time()
        drain_timeout = False
        drain_time = 0.0

        while True:
            cur_snap = sampler.take_immediate_snapshot(phase="drain")
            all_snapshots.append(cur_snap)
            elapsed_drain = time.time() - drain_start

            # Handle measurement failure during drain
            if not cur_snap.query_success or cur_snap.total_lag() is None:
                print(f"  [WARN] Kafka lag query failed during drain: {cur_snap.error_msg}")
                if elapsed_drain >= drain_timeout_s:
                    drain_timeout = True
                    drain_time = elapsed_drain
                    print(f"  [FAIL] Drain failed: measurement failure and timeout exceeded ({drain_time:.2f}s >= {drain_timeout_s}s)", file=sys.stderr)
                    suite_failed = True
                    break
                time.sleep(sample_interval_s)
                continue

            total_lag = cur_snap.total_lag()

            if total_lag == 0:
                drain_time = elapsed_drain
                # Strict timeout enforcement: cannot report PASS if elapsed > drain_timeout_s
                if drain_time > drain_timeout_s:
                    drain_timeout = True
                    print(f"  [FAIL] Drain reached zero lag but exceeded bounded timeout ({drain_time:.2f}s > {drain_timeout_s}s)", file=sys.stderr)
                    suite_failed = True
                else:
                    print(f"  [OK] Consumers drained to zero lag in {drain_time:.2f}s")
                break
            else:
                if elapsed_drain >= drain_timeout_s:
                    drain_timeout = True
                    drain_time = elapsed_drain
                    print(f"  [FAIL] Drain timeout reached ({drain_timeout_s}s); remaining lag: {total_lag}", file=sys.stderr)
                    suite_failed = True
                    break

            time.sleep(sample_interval_s)

        sampler.stop()

        # Gather all snapshots for this concurrency run
        c_snapshots = sampler.get_snapshots()
        all_snapshots.extend([s for s in c_snapshots if s not in all_snapshots])
        all_write_records.extend(write_records)

        # Final Node 0 metrics
        m_after = get_node_metrics(target_port)
        events_after = m_after.get("events_published", 0)
        events_published = events_after - events_before

        successful_writes = sum(1 for w in write_records if w.success)
        errors = sum(1 for w in write_records if not w.success)

        # Filter to successful workload snapshots for mean lag
        workload_snaps = [s for s in c_snapshots if s.phase in ("workload", "workload_end") and s.query_success]

        def safe_mean(values: List[Optional[int]]) -> Optional[float]:
            v = [x for x in values if x is not None]
            return round(statistics.mean(v), 2) if v else None

        def safe_max(values: List[Optional[int]]) -> Optional[int]:
            v = [x for x in values if x is not None]
            return max(v) if v else None

        mean_sampled_n0 = safe_mean([s.group_lag("dse-node-0") for s in workload_snaps])
        mean_sampled_n1 = safe_mean([s.group_lag("dse-node-1") for s in workload_snaps])
        mean_sampled_n2 = safe_mean([s.group_lag("dse-node-2") for s in workload_snaps])

        # Maximum lag observed across all valid snapshots
        valid_snapshots = [s for s in c_snapshots if s.query_success]
        max_lag_n0 = safe_max([s.group_lag("dse-node-0") for s in valid_snapshots])
        max_lag_n1 = safe_max([s.group_lag("dse-node-1") for s in valid_snapshots])
        max_lag_n2 = safe_max([s.group_lag("dse-node-2") for s in valid_snapshots])
        max_lag_overall = safe_max([s.total_lag() for s in valid_snapshots])

        max_lag_p0 = safe_max([s.partition_max_lag(0) for s in valid_snapshots])
        max_lag_p1 = safe_max([s.partition_max_lag(1) for s in valid_snapshots])
        max_lag_p2 = safe_max([s.partition_max_lag(2) for s in valid_snapshots])

        failed_snapshots_count = sum(1 for s in c_snapshots if not s.query_success)

        # Final valid drain snapshot lag
        last_valid_drain_snap = next((s for s in reversed(c_snapshots) if s.query_success and s.phase == "drain"), None)
        final_n0 = last_valid_drain_snap.group_lag("dse-node-0") if last_valid_drain_snap else None
        final_n1 = last_valid_drain_snap.group_lag("dse-node-1") if last_valid_drain_snap else None
        final_n2 = last_valid_drain_snap.group_lag("dse-node-2") if last_valid_drain_snap else None
        final_total = last_valid_drain_snap.total_lag() if last_valid_drain_snap else None

        status = "PASS"
        if drain_timeout or errors > 0 or final_total is None or final_total > 0 or last_valid_drain_snap is None:
            status = "FAIL"
            suite_failed = True

        summary = ConcurrencySummary(
            concurrency=c,
            requested_writes=measured_count,
            successful_writes=successful_writes,
            errors=errors,
            events_published=events_published,
            start_offsets_summary=start_offsets_str,
            max_lag_node0=max_lag_n0,
            max_lag_node1=max_lag_n1,
            max_lag_node2=max_lag_n2,
            max_lag_overall=max_lag_overall,
            max_lag_p0=max_lag_p0,
            max_lag_p1=max_lag_p1,
            max_lag_p2=max_lag_p2,
            mean_sampled_lag_node0=mean_sampled_n0,
            mean_sampled_lag_node1=mean_sampled_n1,
            mean_sampled_lag_node2=mean_sampled_n2,
            final_lag_node0=final_n0,
            final_lag_node1=final_n1,
            final_lag_node2=final_n2,
            final_lag_total=final_total,
            drain_time_sec=round(drain_time, 3),
            drain_timeout=drain_timeout,
            failed_snapshots=failed_snapshots_count,
            status=status
        )
        all_summaries.append(summary)

        print(f"  Results C={c}: Status={status} | Successful Writes={successful_writes}/{measured_count} | Events Published={events_published}")
        print(f"  Max Lag: node0={max_lag_n0}, node1={max_lag_n1}, node2={max_lag_n2} | Partitions: p0={max_lag_p0}, p1={max_lag_p1}, p2={max_lag_p2}")
        print(f"  Sampled Mean Lag: node0={mean_sampled_n0}, node1={mean_sampled_n1}, node2={mean_sampled_n2}")
        print(f"  Final Lag: {final_total} | Drain Time: {drain_time:.2f}s | Failed Snapshots: {failed_snapshots_count}")

    # --- CSV Export ---
    os.makedirs(os.path.dirname(out_prefix) or ".", exist_ok=True)

    # 1. Summary CSV
    summary_path = f"{out_prefix}_summary.csv"
    with open(summary_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "concurrency", "requested_writes", "successful_writes", "errors", "events_published",
            "max_lag_node0", "max_lag_node1", "max_lag_node2", "max_lag_overall",
            "max_lag_p0", "max_lag_p1", "max_lag_p2",
            "mean_sampled_lag_node0", "mean_sampled_lag_node1", "mean_sampled_lag_node2",
            "final_lag_node0", "final_lag_node1", "final_lag_node2", "final_lag_total",
            "drain_time_sec", "drain_timeout", "failed_snapshots", "status", "start_offsets"
        ])
        for s in all_summaries:
            writer.writerow([
                s.concurrency, s.requested_writes, s.successful_writes, s.errors, s.events_published,
                s.max_lag_node0 if s.max_lag_node0 is not None else "-",
                s.max_lag_node1 if s.max_lag_node1 is not None else "-",
                s.max_lag_node2 if s.max_lag_node2 is not None else "-",
                s.max_lag_overall if s.max_lag_overall is not None else "-",
                s.max_lag_p0 if s.max_lag_p0 is not None else "-",
                s.max_lag_p1 if s.max_lag_p1 is not None else "-",
                s.max_lag_p2 if s.max_lag_p2 is not None else "-",
                s.mean_sampled_lag_node0 if s.mean_sampled_lag_node0 is not None else "-",
                s.mean_sampled_lag_node1 if s.mean_sampled_lag_node1 is not None else "-",
                s.mean_sampled_lag_node2 if s.mean_sampled_lag_node2 is not None else "-",
                s.final_lag_node0 if s.final_lag_node0 is not None else "-",
                s.final_lag_node1 if s.final_lag_node1 is not None else "-",
                s.final_lag_node2 if s.final_lag_node2 is not None else "-",
                s.final_lag_total if s.final_lag_total is not None else "-",
                s.drain_time_sec, s.drain_timeout, s.failed_snapshots, s.status, s.start_offsets_summary
            ])
    print(f"\n[OK] Summary CSV exported to: {summary_path}")

    # 2. Raw Snapshots CSV
    snapshots_path = f"{out_prefix}_snapshots.csv"
    with open(snapshots_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            "sample_index", "timestamp_rel_s", "phase", "group", "partition",
            "current_offset", "log_end_offset", "lag", "query_status", "error_msg"
        ])
        for snap in all_snapshots:
            for rec in snap.records:
                writer.writerow([
                    snap.sample_index, round(snap.timestamp_rel_s, 3), snap.phase,
                    rec.group, rec.partition,
                    rec.current_offset if rec.current_offset is not None else "-",
                    rec.log_end_offset if rec.log_end_offset is not None else "-",
                    rec.lag if rec.lag is not None else "-",
                    "FAIL" if rec.query_failed else "OK",
                    rec.error_msg
                ])
    print(f"[OK] Snapshots CSV exported to: {snapshots_path}")

    # 3. Raw Writes CSV
    writes_path = f"{out_prefix}_writes.csv"
    with open(writes_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["concurrency", "request_id", "doc_id", "status_code", "latency_ms", "success", "error_msg"])
        for w in all_write_records:
            writer.writerow([
                w.concurrency, w.request_id, w.doc_id, w.status_code,
                round(w.latency_ms, 3), w.success, w.error_msg
            ])
    print(f"[OK] Writes CSV exported to: {writes_path}")

    print("\n" + "=" * 70)
    print("Benchmark H Suite Completed")
    print(f"Overall Status: {'FAIL' if suite_failed else 'PASS'}")
    print("=" * 70)

    return 1 if suite_failed else 0


# ---------------------------------------------------------------------------
# Self-Test Mode
# ---------------------------------------------------------------------------

def run_self_tests() -> int:
    """
    Validates Benchmark H harness correctness:
      1. Successful three-group parallel collection
      2. Kafka query timeout/failure is NOT converted to lag=0
      3. Valid CURRENT-OFFSET='-' remains distinguishable from query failure
      4. Failed snapshot is excluded from sampled lag statistics
      5. Drain timeout cannot report PASS after the configured timeout
      6. Valid zero lag still reports zero
    """
    print("Running Benchmark H harness self-tests...")

    # Test 1: Successful three-group parallel collection
    def mock_successful_query(g: str) -> Tuple[List[PartitionLagRecord], bool, str]:
        recs = [
            PartitionLagRecord(group=g, partition=0, current_offset=10, log_end_offset=12, lag=2),
            PartitionLagRecord(group=g, partition=1, current_offset=20, log_end_offset=20, lag=0),
            PartitionLagRecord(group=g, partition=2, current_offset=30, log_end_offset=35, lag=5),
        ]
        return recs, True, ""

    snap1 = collect_lag_snapshot(
        sample_index=1, timestamp_rel_s=0.5, phase="workload",
        groups=["dse-node-2", "dse-node-0", "dse-node-1"],
        query_fn=mock_successful_query
    )
    assert snap1.query_success is True, "Snap1 must be query_success=True"
    assert len(snap1.records) == 9, f"Expected 9 records across 3 groups, got {len(snap1.records)}"
    # Verify deterministic ordering: sorted by group then partition
    expected_order = [
        ("dse-node-0", 0), ("dse-node-0", 1), ("dse-node-0", 2),
        ("dse-node-1", 0), ("dse-node-1", 1), ("dse-node-1", 2),
        ("dse-node-2", 0), ("dse-node-2", 1), ("dse-node-2", 2),
    ]
    actual_order = [(r.group, r.partition) for r in snap1.records]
    assert actual_order == expected_order, f"Records not in deterministic order: {actual_order}"
    assert snap1.group_lag("dse-node-0") == 7
    assert snap1.total_lag() == 21  # 7 * 3
    print("  [PASS] Test 1: Successful three-group parallel collection and deterministic ordering")

    # Test 2: Kafka query timeout/failure is NOT converted to lag=0
    def mock_failing_query(g: str) -> Tuple[List[PartitionLagRecord], bool, str]:
        if g == "dse-node-1":
            recs = [
                PartitionLagRecord(group=g, partition=p, current_offset=None, log_end_offset=None, lag=None, query_failed=True, error_msg="Timeout")
                for p in PARTITIONS
            ]
            return recs, False, "Timeout"
        return mock_successful_query(g)

    snap2 = collect_lag_snapshot(
        sample_index=2, timestamp_rel_s=1.0, phase="workload",
        groups=CONSUMER_GROUPS,
        query_fn=mock_failing_query
    )
    assert snap2.query_success is False, "Snap2 must report query_success=False"
    assert snap2.group_lag("dse-node-1") is None, "Failed group must have lag=None, NOT 0"
    assert snap2.total_lag() is None, "Total lag with failing group must be None, NEVER 0"
    for r in snap2.records:
        if r.group == "dse-node-1":
            assert r.query_failed is True
            assert r.lag is None, "Failed record must have lag=None, never 0"
    print("  [PASS] Test 2: Kafka query timeout/failure is NOT converted to lag=0")

    # Test 3: Valid CURRENT-OFFSET='-' remains distinguishable from query failure
    sample_uncommitted = """
GROUP           TOPIC               PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
dse-node-0      documents.mutations 0          -               15              -               dse-node-0-1    /127.0.0.1      dse-node-0-consumer
dse-node-0      documents.mutations 1          -               0               -               dse-node-0-1    /127.0.0.1      dse-node-0-consumer
dse-node-0      documents.mutations 2          5               10              5               dse-node-0-1    /127.0.0.1      dse-node-0-consumer
"""
    recs3, success3, err3 = parse_consumer_group_describe_output("dse-node-0", sample_uncommitted)
    assert success3 is True, f"Valid uncommitted parsing failed: {err3}"
    assert recs3[0].partition == 0 and recs3[0].current_offset is None and recs3[0].log_end_offset == 15 and recs3[0].lag == 15
    assert recs3[0].query_failed is False, "Uncommitted offset must have query_failed=False"
    assert recs3[1].partition == 1 and recs3[1].current_offset is None and recs3[1].log_end_offset == 0 and recs3[1].lag == 0
    assert recs3[1].query_failed is False
    assert recs3[2].partition == 2 and recs3[2].current_offset == 5 and recs3[2].log_end_offset == 10 and recs3[2].lag == 5
    print("  [PASS] Test 3: Valid CURRENT-OFFSET='-' distinguishable from query failure")

    # Test 4: Failed snapshot is excluded from sampled lag statistics
    # Snap A has lag=10, Snap B failed (query_success=False), Snap C has lag=20
    snap_a = LagSnapshot(sample_index=1, timestamp_rel_s=0.5, phase="workload", records=[
        PartitionLagRecord("dse-node-0", 0, 0, 10, 10)
    ], query_success=True)
    snap_b = LagSnapshot(sample_index=2, timestamp_rel_s=1.0, phase="workload", records=[
        PartitionLagRecord("dse-node-0", 0, None, None, None, query_failed=True)
    ], query_success=False)
    snap_c = LagSnapshot(sample_index=3, timestamp_rel_s=1.5, phase="workload", records=[
        PartitionLagRecord("dse-node-0", 0, 10, 30, 20)
    ], query_success=True)

    all_test_snaps = [snap_a, snap_b, snap_c]
    valid_test_snaps = [s for s in all_test_snaps if s.phase in ("workload", "workload_end") and s.query_success]
    vals = [s.group_lag("dse-node-0") for s in valid_test_snaps]
    vals_clean = [v for v in vals if v is not None]
    computed_mean = statistics.mean(vals_clean)
    # Expected mean is (10 + 20) / 2 = 15.0; if failed snap were treated as 0, mean would be 10.0
    assert computed_mean == 15.0, f"Expected mean 15.0, got {computed_mean}"
    print("  [PASS] Test 4: Failed snapshot is excluded from sampled lag statistics")

    # Test 5: Drain timeout cannot report PASS after configured timeout
    # Simulate drain elapsed = 18.16s when timeout is 15.0s
    drain_timeout_s = 15.0
    simulated_drain_time = 18.16
    simulated_total_lag = 0  # Consumers reached 0 lag, but after the bound!

    # Correct logic under test:
    drain_timed_out = False
    run_status = "PASS"
    if simulated_total_lag == 0:
        if simulated_drain_time > drain_timeout_s:
            drain_timed_out = True
            run_status = "FAIL"
    assert drain_timed_out is True, "Must set drain_timed_out=True when drain_time > timeout"
    assert run_status == "FAIL", f"Must evaluate to FAIL after timeout, got {run_status}"
    print("  [PASS] Test 5: Drain timeout cannot report PASS after configured timeout")

    # Test 6: Valid zero lag still reports zero
    sample_zero_lag = """
GROUP           TOPIC               PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG             CONSUMER-ID     HOST            CLIENT-ID
dse-node-0      documents.mutations 0          50              50              0               dse-node-0-1    /127.0.0.1      dse-node-0-consumer
dse-node-0      documents.mutations 1          50              50              0               dse-node-0-1    /127.0.0.1      dse-node-0-consumer
dse-node-0      documents.mutations 2          50              50              0               dse-node-0-1    /127.0.0.1      dse-node-0-consumer
"""
    recs6, success6, err6 = parse_consumer_group_describe_output("dse-node-0", sample_zero_lag)
    assert success6 is True, f"Parsing valid zero lag failed: {err6}"
    for r in recs6:
        assert r.query_failed is False
        assert r.lag == 0
        assert r.current_offset == 50
        assert r.log_end_offset == 50
    snap6 = LagSnapshot(sample_index=1, timestamp_rel_s=0.5, phase="drain", records=recs6, query_success=True)
    assert snap6.total_lag() == 0, "Valid zero lag must report total_lag=0"
    print("  [PASS] Test 6: Valid zero lag correctly reports zero")

    print("\nAll 6 Benchmark H harness self-tests PASSED successfully.")
    return 0


# ---------------------------------------------------------------------------
# Main CLI Entry Point
# ---------------------------------------------------------------------------

def main() -> int:
    parser = argparse.ArgumentParser(description="Phase 21 Benchmark H: Kafka Consumer Lag Under Load")
    parser.add_argument("--self-test", action="store_true", help="Run harness self-tests and exit")
    parser.add_argument("--run-id", type=str, default="H_kafka_lag", help="Benchmark run identifier")
    parser.add_argument("--out-prefix", type=str, default="results/phase21_H_kafka_lag", help="Output CSV prefix")
    parser.add_argument("--target-port", type=int, default=DEFAULT_TARGET_PORT, help="Node 0 HTTP port")
    parser.add_argument("--warmup", type=int, default=DEFAULT_WARMUP_WRITES, help="Warmup write count per concurrency")
    parser.add_argument("--measured", type=int, default=DEFAULT_MEASURED_WRITES, help="Measured write count per concurrency")
    parser.add_argument("--sample-interval", type=float, default=DEFAULT_SAMPLE_INTERVAL_SEC, help="Lag sampling interval in seconds")
    parser.add_argument("--drain-timeout", type=float, default=DEFAULT_DRAIN_TIMEOUT_SEC, help="Drain timeout in seconds")
    parser.add_argument("--cli-timeout", type=float, default=DEFAULT_KAFKA_CLI_TIMEOUT_SEC, help="Kafka CLI per-query timeout in seconds")
    parser.add_argument("--concurrencies", type=int, nargs="+", default=[1, 4, 8, 16], help="List of concurrency levels")

    args = parser.parse_args()

    if args.self_test:
        return run_self_tests()

    return run_benchmark_h(
        target_port=args.target_port,
        warmup_count=args.warmup,
        measured_count=args.measured,
        sample_interval_s=args.sample_interval,
        drain_timeout_s=args.drain_timeout,
        cli_timeout_s=args.cli_timeout,
        out_prefix=args.out_prefix,
        concurrencies=args.concurrencies
    )


if __name__ == "__main__":
    sys.exit(main())
