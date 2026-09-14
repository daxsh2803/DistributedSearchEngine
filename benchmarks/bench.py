#!/usr/bin/env python3
# benchmarks/bench.py
#
# Phase 21 — Distributed Search Engine Benchmark Harness
#
# Measures latency, throughput, p50/p95/p99 for index, search, and mixed
# workloads against the DSE HTTP API.
#
# Requirements:
#   Python >= 3.13 (standard library only — no pip installs)
#
# Usage:
#   python benchmarks/bench.py --help
#
# Example (smoke test):
#   python benchmarks/bench.py \
#       --url http://127.0.0.1:8080 \
#       --benchmark index \
#       --payload S \
#       --concurrency 1 \
#       --requests 10 \
#       --warmup 5 \
#       --output results/smoke
#
# Example (full concurrency sweep):
#   python benchmarks/bench.py \
#       --url http://127.0.0.1:8080 \
#       --benchmark search \
#       --concurrency 1,2,4,8,16 \
#       --requests 1000 \
#       --warmup 50 \
#       --output results/B_search
#
# Design notes:
#   - One persistent HTTPConnection per worker thread (keep-alive).
#     Connection-setup cost is NOT included in measured latency after warm-up.
#   - Warm-up requests are discarded entirely.
#   - Latency measured with time.perf_counter() (nanosecond resolution).
#   - Throughput = total_requests / wall_clock_elapsed (not sum of latencies).
#   - p50/p95/p99 use statistics.quantiles(..., n=100, method="inclusive").
#   - Document IDs are globally unique per run (never reused).
#   - Query terms cycle deterministically over the approved 10-term set.
#   - All errors are recorded; harness never crashes on individual request failure.

import argparse
import csv
import dataclasses
import http.client
import json
import pathlib
import statistics
import sys
import threading
import time
import urllib.parse
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Optional


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Approved query set for search benchmarks (Task 3, Phase 21 Design).
QUERY_TERMS = [
    "fox", "quick", "search", "document", "distributed",
    "index", "kafka", "node", "shard", "engine",
]

# Fixed document payloads (Task 3, Phase 21 Design).
PAYLOADS = {
    "S": "the quick brown fox jumps over the lazy dog",
    "M": (
        "distributed search engines index large document collections using "
        "inverted index structures with tf-idf scoring algorithms for ranked "
        "retrieval supporting boolean query modes including or and and with "
        "concurrent multi-shard fan-out across replicated node clusters"
    ),
}

# HTTP success codes per operation.
SUCCESS_CODE = {
    "POST":   201,
    "GET":    200,
    "PUT":    200,
    "DELETE": 204,
}

VERSION = "1.0.0"


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class RequestRecord:
    """Per-request measurement record written to the per-request CSV."""
    run_id:      str
    benchmark:   str
    payload:     str
    concurrency: int
    request_id:  int
    method:      str
    path:        str
    status_code: int     # 0 if transport error
    success:     bool
    latency_ms:  float
    error:       str


@dataclass
class RunSummary:
    """Aggregate statistics for one concurrency level."""
    run_id:                str
    benchmark:             str
    payload:               str
    concurrency:           int
    request_count:         int
    completed:             int
    success:               int
    errors:                int
    p50_ms:                float
    p95_ms:                float
    p99_ms:                float
    mean_ms:               float
    total_elapsed_seconds: float
    throughput_rps:        float
    server_metrics_before: str
    server_metrics_after:  str


# ---------------------------------------------------------------------------
# ID generation — globally unique per run, thread-safe
# ---------------------------------------------------------------------------

class DocIdGenerator:
    """Thread-safe monotonic document ID generator.

    IDs are assigned in the order workers call next_id().
    Using a base offset derived from --id-base ensures IDs do not collide
    across repeated runs against the same server instance.

    The server rejects duplicate IDs with 409 Conflict, so uniqueness is
    validated implicitly by the success/error counts in the results.
    """

    def __init__(self, base_offset: int = 0):
        self._lock = threading.Lock()
        self._next = base_offset + 1

    def next_id(self) -> int:
        with self._lock:
            val = self._next
            self._next += 1
            return val


# ---------------------------------------------------------------------------
# HTTP connection — persistent, per-worker
# ---------------------------------------------------------------------------

class WorkerConnection:
    """Persistent HTTP connection for a single worker thread.

    Wraps http.client.HTTPConnection.  On transport failure the broken
    connection is closed and a RuntimeError is raised.  The caller records
    the error; the harness never silently retries application requests.
    """

    def __init__(self, host: str, port: int, timeout: float = 10.0):
        self._host = host
        self._port = port
        self._timeout = timeout
        self._conn: Optional[http.client.HTTPConnection] = None
        self._connect()

    def _connect(self) -> None:
        """Open a fresh TCP connection."""
        try:
            self._conn = http.client.HTTPConnection(
                self._host, self._port, timeout=self._timeout
            )
            self._conn.connect()
        except Exception:
            self._conn = None

    def request(
        self,
        method: str,
        path: str,
        body: Optional[str] = None,
        headers: Optional[dict] = None,
    ) -> tuple[int, str]:
        """Send a single request and return (status_code, response_body).

        Raises RuntimeError on transport failure.
        Does NOT automatically retry.
        """
        if headers is None:
            headers = {}
        if body is not None:
            encoded = body.encode("utf-8")
            headers["Content-Type"] = "application/json"
            headers["Content-Length"] = str(len(encoded))

        try:
            if self._conn is None:
                self._connect()
                if self._conn is None:
                    raise RuntimeError("Could not establish connection to server")
            self._conn.request(method, path, body=body, headers=headers)
            resp = self._conn.getresponse()
            status = resp.status
            resp_body = resp.read().decode("utf-8", errors="replace")
            return status, resp_body
        except Exception as exc:
            # Transport failure: close the broken connection.
            self._close()
            raise RuntimeError(str(exc)) from exc

    def _close(self) -> None:
        if self._conn is not None:
            try:
                self._conn.close()
            except Exception:
                pass
            self._conn = None

    def close(self) -> None:
        self._close()


# ---------------------------------------------------------------------------
# Request builders
# ---------------------------------------------------------------------------

def _build_search_path(query_index: int) -> str:
    term = QUERY_TERMS[query_index % len(QUERY_TERMS)]
    params = urllib.parse.urlencode({"q": term, "mode": "or"})
    return f"/search?{params}"


def _build_post_body(doc_id: int, content: str) -> str:
    return json.dumps({"id": doc_id, "content": content})


def _is_search_success(status: int, body: str) -> bool:
    """Search succeeds when HTTP 200, valid JSON, and response['total'] > 0."""
    if status != 200:
        return False
    try:
        data = json.loads(body)
        total = data.get("total")
        return isinstance(total, int) and total > 0
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Task list builder
# ---------------------------------------------------------------------------

def _build_task_list(
    benchmark: str,
    count: int,
    start_request_id: int,
    id_gen: DocIdGenerator,
) -> list[tuple[int, int]]:
    """Return list of (request_id, aux_id) tuples.

    For index  : aux_id = unique doc_id from id_gen
    For search : aux_id = deterministic query term index
    For mixed  : aux_id = doc_id for writes, query_index for reads
                  (70% writes on positions 0-6 of each 10-cycle)
    """
    tasks: list[tuple[int, int]] = []
    for i in range(count):
        req_id = start_request_id + i
        if benchmark == "index":
            aux = id_gen.next_id()
        elif benchmark == "search":
            aux = (start_request_id + i) % len(QUERY_TERMS)
        else:  # mixed
            pos = req_id % 10
            if pos < 7:
                aux = id_gen.next_id()           # write
            else:
                aux = req_id % len(QUERY_TERMS)  # read
        tasks.append((req_id, aux))
    return tasks


def _split_tasks(
    tasks: list[tuple[int, int]], concurrency: int
) -> list[list[tuple[int, int]]]:
    """Distribute tasks round-robin across `concurrency` worker slots."""
    concurrency = max(1, concurrency)
    chunks: list[list[tuple[int, int]]] = [[] for _ in range(concurrency)]
    for i, task in enumerate(tasks):
        chunks[i % concurrency].append(task)
    return chunks


# ---------------------------------------------------------------------------
# Worker function
# ---------------------------------------------------------------------------

def _run_worker(
    *,
    host: str,
    port: int,
    benchmark: str,
    content: str,
    task_queue: list[tuple[int, int]],
    is_warmup: bool,
    records_out: list,
    records_lock: threading.Lock,
    timeout: float,
) -> None:
    """Worker thread: processes its slice of tasks sequentially.

    Uses a single persistent connection for all assigned tasks.
    Appends RequestRecord objects to records_out under records_lock
    (only for non-warmup tasks).
    """
    conn = WorkerConnection(host, port, timeout=timeout)
    try:
        for request_id, aux_id in task_queue:
            # Determine operation ---
            if benchmark == "index":
                method = "POST"
                path = "/documents"
                body: Optional[str] = _build_post_body(aux_id, content)
                expected_status = SUCCESS_CODE["POST"]
                is_write = True
            elif benchmark == "search":
                method = "GET"
                path = _build_search_path(aux_id)
                body = None
                expected_status = SUCCESS_CODE["GET"]
                is_write = False
            else:  # mixed
                pos = request_id % 10
                if pos < 7:
                    method = "POST"
                    path = "/documents"
                    body = _build_post_body(aux_id, content)
                    expected_status = SUCCESS_CODE["POST"]
                    is_write = True
                else:
                    method = "GET"
                    path = _build_search_path(aux_id)
                    body = None
                    expected_status = SUCCESS_CODE["GET"]
                    is_write = False

            # Measure ---
            status = 0
            resp_body = ""
            error_str = ""
            success = False

            t0 = time.perf_counter()
            try:
                status, resp_body = conn.request(method, path, body)
                if not is_write:
                    success = _is_search_success(status, resp_body)
                else:
                    success = (status == expected_status)
            except RuntimeError as exc:
                error_str = str(exc)
                success = False
            t1 = time.perf_counter()
            latency_ms = (t1 - t0) * 1000.0

            if not is_warmup:
                # run_id and concurrency filled later by the outer function
                rec = RequestRecord(
                    run_id="",
                    benchmark=benchmark,
                    payload="",
                    concurrency=0,
                    request_id=request_id,
                    method=method,
                    path=path,
                    status_code=status,
                    success=success,
                    latency_ms=round(latency_ms, 4),
                    error=error_str,
                )
                with records_lock:
                    records_out.append(rec)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Metrics snapshot helpers
# ---------------------------------------------------------------------------

def fetch_metrics(host: str, port: int, timeout: float = 5.0) -> str:
    """Fetch /metrics. Returns JSON string or empty string on failure."""
    try:
        conn = http.client.HTTPConnection(host, port, timeout=timeout)
        conn.request("GET", "/metrics")
        resp = conn.getresponse()
        body = resp.read().decode("utf-8", errors="replace")
        conn.close()
        return body if resp.status == 200 else ""
    except Exception:
        return ""


def health_check(host: str, port: int, timeout: float = 5.0) -> bool:
    """Return True if GET /health returns HTTP 200."""
    try:
        conn = http.client.HTTPConnection(host, port, timeout=timeout)
        conn.request("GET", "/health")
        resp = conn.getresponse()
        resp.read()
        conn.close()
        return resp.status == 200
    except Exception:
        return False


def _abbreviate_metrics(metrics_json: str) -> str:
    """Extract coordinator-level fields from /metrics JSON for summary storage.

    Returns a compact JSON string to avoid huge per-node blobs in CSV.
    """
    if not metrics_json:
        return ""
    try:
        d = json.loads(metrics_json)
        keys = [
            "coordinator_searches_total", "coordinator_search_success",
            "coordinator_search_errors", "coordinator_search_latency",
            "coordinator_writes_total", "coordinator_write_success",
            "coordinator_write_errors", "coordinator_write_latency",
            "events_total", "events_published", "events_failed",
        ]
        brief = {k: d[k] for k in keys if k in d}
        return json.dumps(brief)
    except Exception:
        return metrics_json[:500]


# ---------------------------------------------------------------------------
# Percentile calculation
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


# ---------------------------------------------------------------------------
# Core benchmark runner (single concurrency level)
# ---------------------------------------------------------------------------

def run_benchmark(
    *,
    run_id: str,
    host: str,
    port: int,
    benchmark: str,
    payload_key: str,
    concurrency: int,
    request_count: int,
    warmup_count: int,
    id_gen: DocIdGenerator,
    timeout: float = 10.0,
    verbose: bool = True,
) -> tuple[list[RequestRecord], RunSummary]:
    """Execute warm-up + measured workload at one concurrency level.

    Returns (per_request_records, summary).
    """
    content = PAYLOADS.get(payload_key, PAYLOADS["S"])

    # --- Warm-up ---
    if warmup_count > 0:
        if verbose:
            print(f"    Warm-up: {warmup_count} requests (discarded) ...")
        warmup_tasks = _build_task_list(benchmark, warmup_count, 0, id_gen)
        warmup_lock = threading.Lock()
        warmup_records: list = []
        chunks = _split_tasks(warmup_tasks, concurrency)
        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [
                pool.submit(
                    _run_worker,
                    host=host, port=port, benchmark=benchmark,
                    content=content, task_queue=chunk,
                    is_warmup=True, records_out=warmup_records,
                    records_lock=warmup_lock, timeout=timeout,
                )
                for chunk in chunks if chunk
            ]
            for f in futures:
                f.result()
        if verbose:
            print(f"    Warm-up complete.")

    # --- Measured ---
    if verbose:
        print(f"    Measured: {request_count} requests @ concurrency={concurrency} ...")

    meas_tasks = _build_task_list(benchmark, request_count, warmup_count, id_gen)
    records: list[RequestRecord] = []
    records_lock = threading.Lock()

    metrics_before = fetch_metrics(host, port)
    wall_start = time.perf_counter()

    chunks = _split_tasks(meas_tasks, concurrency)
    with ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [
            pool.submit(
                _run_worker,
                host=host, port=port, benchmark=benchmark,
                content=content, task_queue=chunk,
                is_warmup=False, records_out=records,
                records_lock=records_lock, timeout=timeout,
            )
            for chunk in chunks if chunk
        ]
        for f in futures:
            try:
                f.result()
            except Exception as exc:
                print(f"    [WARNING] Worker raised: {exc}", file=sys.stderr)

    wall_end = time.perf_counter()
    total_elapsed = wall_end - wall_start
    metrics_after = fetch_metrics(host, port)

    # Fill per-record run metadata (kept out of worker threads for simplicity)
    for rec in records:
        rec.run_id = run_id
        rec.payload = payload_key
        rec.concurrency = concurrency

    # Compute statistics
    completed = len(records)
    success_count = sum(1 for r in records if r.success)
    error_count = completed - success_count
    success_latencies = [r.latency_ms for r in records if r.success]

    p50, p95, p99, mean_ms = compute_percentiles(success_latencies)
    throughput = completed / total_elapsed if total_elapsed > 0 else 0.0

    summary = RunSummary(
        run_id=run_id,
        benchmark=benchmark,
        payload=payload_key,
        concurrency=concurrency,
        request_count=request_count,
        completed=completed,
        success=success_count,
        errors=error_count,
        p50_ms=round(p50, 3),
        p95_ms=round(p95, 3),
        p99_ms=round(p99, 3),
        mean_ms=round(mean_ms, 3),
        total_elapsed_seconds=round(total_elapsed, 4),
        throughput_rps=round(throughput, 2),
        server_metrics_before=_abbreviate_metrics(metrics_before),
        server_metrics_after=_abbreviate_metrics(metrics_after),
    )

    if verbose:
        print(f"    Completed={completed}  Success={success_count}  Errors={error_count}")
        print(
            f"    p50={p50:.2f}ms  p95={p95:.2f}ms  p99={p99:.2f}ms  "
            f"mean={mean_ms:.2f}ms"
        )
        print(
            f"    Elapsed={total_elapsed:.3f}s  Throughput={throughput:.1f} req/s"
        )

    return records, summary


# ---------------------------------------------------------------------------
# CSV output
# ---------------------------------------------------------------------------

_REQUEST_FIELDS = [
    "run_id", "benchmark", "payload", "concurrency",
    "request_id", "method", "path", "status_code",
    "success", "latency_ms", "error",
]

_SUMMARY_FIELDS = [
    "run_id", "benchmark", "payload", "concurrency",
    "request_count", "completed", "success", "errors",
    "p50_ms", "p95_ms", "p99_ms", "mean_ms",
    "total_elapsed_seconds", "throughput_rps",
    "server_metrics_before", "server_metrics_after",
]


def write_request_csv(records: list[RequestRecord], path: pathlib.Path) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_REQUEST_FIELDS)
        writer.writeheader()
        for r in records:
            writer.writerow(dataclasses.asdict(r))


def write_summary_csv(summaries: list[RunSummary], path: pathlib.Path) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=_SUMMARY_FIELDS)
        writer.writeheader()
        for s in summaries:
            writer.writerow(dataclasses.asdict(s))


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="bench.py",
        description=(
            "Phase 21 — Distributed Search Engine Benchmark Harness\n"
            "Measures latency, throughput, and p50/p95/p99 for DSE HTTP API.\n\n"
            "Benchmark types:\n"
            "  index   POST /documents (inserts documents)\n"
            "  search  GET  /search    (searches across shards)\n"
            "  mixed   70%% writes + 30%% searches\n\n"
            "Payload sizes:\n"
            "  S  Small    (~44 chars, ~6 unique tokens)\n"
            "  M  Moderate (~250 chars, ~30 unique tokens)\n\n"
            "Examples:\n"
            "  # Smoke test\n"
            "  python benchmarks/bench.py \\\n"
            "      --url http://127.0.0.1:8080 \\\n"
            "      --benchmark index --payload S \\\n"
            "      --concurrency 1 --requests 10 --warmup 5 \\\n"
            "      --output results/smoke\n\n"
            "  # Concurrency sweep\n"
            "  python benchmarks/bench.py \\\n"
            "      --url http://127.0.0.1:8080 \\\n"
            "      --benchmark search \\\n"
            "      --concurrency 1,2,4,8,16 \\\n"
            "      --requests 1000 --warmup 50 \\\n"
            "      --output results/B_search"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--url",
        required=True,
        metavar="URL",
        help="Base URL of the DSE server, e.g. http://127.0.0.1:8080",
    )
    parser.add_argument(
        "--benchmark",
        required=True,
        choices=["index", "search", "mixed"],
        metavar="TYPE",
        help="Benchmark type: index | search | mixed",
    )
    parser.add_argument(
        "--payload",
        default="S",
        choices=["S", "M"],
        metavar="SIZE",
        help=(
            "Document payload size for index/mixed: S (small) or M (moderate). "
            "Default: S. Ignored for pure search benchmarks."
        ),
    )
    parser.add_argument(
        "--concurrency",
        default="1",
        metavar="N[,N,...]",
        help=(
            "Comma-separated concurrency levels. Each is a separate sub-run. "
            "Example: --concurrency 1,2,4,8,16. Default: 1."
        ),
    )
    parser.add_argument(
        "--requests",
        type=int,
        default=100,
        metavar="N",
        help="Number of measured requests per concurrency level. Default: 100.",
    )
    parser.add_argument(
        "--warmup",
        type=int,
        default=10,
        metavar="N",
        help="Number of warm-up requests (discarded, not measured). Default: 10.",
    )
    parser.add_argument(
        "--output",
        required=True,
        metavar="PREFIX",
        help=(
            "Output file prefix. Creates <PREFIX>_requests.csv and "
            "<PREFIX>_summary.csv. Parent dirs created automatically."
        ),
    )
    parser.add_argument(
        "--timeout",
        type=float,
        default=10.0,
        metavar="SECONDS",
        help="Per-request HTTP timeout in seconds. Default: 10.",
    )
    parser.add_argument(
        "--id-base",
        type=int,
        default=0,
        metavar="N",
        help=(
            "Starting document ID offset. Use to avoid ID collisions across "
            "multiple harness runs on the same server. Default: 0 (IDs start at 1)."
        ),
    )
    parser.add_argument(
        "--run-id",
        default="",
        metavar="ID",
        help=(
            "Explicit run identifier written to CSV. "
            "Auto-generated (8-char UUID prefix) if not specified."
        ),
    )
    parser.add_argument(
        "--no-health-check",
        action="store_true",
        default=False,
        help="Skip the pre-benchmark /health check. Not recommended.",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"bench.py {VERSION}",
    )

    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main(argv=None) -> int:  # noqa: C901  (acceptable complexity for CLI entry point)
    args = parse_args(argv)

    # --- Parse URL ---
    parsed = urllib.parse.urlparse(args.url)
    if parsed.scheme not in ("http",):
        print(
            f"ERROR: Unsupported scheme '{parsed.scheme}'. "
            "Only http is supported (DSE uses plain HTTP).",
            file=sys.stderr,
        )
        return 1
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 8080

    # --- Parse concurrency ---
    concurrency_levels: list[int] = []
    for part in args.concurrency.split(","):
        part = part.strip()
        if not part:
            continue
        try:
            c = int(part)
            if c < 1:
                raise ValueError("must be >= 1")
            concurrency_levels.append(c)
        except ValueError as e:
            print(f"ERROR: Invalid concurrency '{part}': {e}", file=sys.stderr)
            return 1
    if not concurrency_levels:
        print("ERROR: No valid concurrency levels.", file=sys.stderr)
        return 1

    # --- Validate counts ---
    if args.requests < 1:
        print("ERROR: --requests must be >= 1.", file=sys.stderr)
        return 1
    if args.warmup < 0:
        print("ERROR: --warmup must be >= 0.", file=sys.stderr)
        return 1

    # --- Prepare output paths ---
    prefix = pathlib.Path(args.output)
    prefix.parent.mkdir(parents=True, exist_ok=True)
    request_csv = pathlib.Path(str(prefix) + "_requests.csv")
    summary_csv = pathlib.Path(str(prefix) + "_summary.csv")

    # --- Run ID ---
    run_id = args.run_id if args.run_id else str(uuid.uuid4())[:8]

    # --- Banner ---
    print()
    print("=" * 62)
    print(f"  DSE Benchmark Harness  v{VERSION}")
    print("=" * 62)
    print(f"  Target      : {args.url}")
    print(f"  Benchmark   : {args.benchmark}")
    print(f"  Payload     : {args.payload}  ({PAYLOADS.get(args.payload,'?')[:40]}...)")
    print(f"  Concurrency : {concurrency_levels}")
    print(f"  Requests    : {args.requests} measured + {args.warmup} warm-up")
    print(f"  Run ID      : {run_id}")
    print(f"  Output      : {prefix}_{{requests,summary}}.csv")
    print("=" * 62)

    # --- Health check ---
    if not args.no_health_check:
        print(f"\n[1/3] Health check -> {args.url}/health")
        if not health_check(host, port, timeout=args.timeout):
            print(
                "\nERROR: Server unreachable or /health did not return HTTP 200.\n"
                f"       Ensure DistributedSearchEngine.exe is running at {args.url}\n"
                "       Use --no-health-check to skip this step.",
                file=sys.stderr,
            )
            return 1
        print("       OK")

    # --- Shared ID generator ---
    # If no explicit --id-base is given, derive one from the current epoch so
    # that every harness invocation automatically uses a distinct ID namespace.
    # This prevents HTTP 409 Conflict from documents created in previous runs.
    # Multiplied by 100000 to leave room for large (up to 100k) request counts.
    id_base = args.id_base if args.id_base != 0 else int(time.time()) * 100_000
    id_gen = DocIdGenerator(base_offset=id_base)
    print(f"  ID base     : {id_base} (IDs start at {id_base + 1})")

    # --- Run all concurrency levels ---
    all_records: list[RequestRecord] = []
    all_summaries: list[RunSummary] = []
    total_levels = len(concurrency_levels)

    print(f"\n[2/3] Running {total_levels} concurrency level(s)...")
    for idx, concurrency in enumerate(concurrency_levels):
        print(f"\n  [{idx + 1}/{total_levels}] concurrency={concurrency}")
        records, summary = run_benchmark(
            run_id=run_id,
            host=host,
            port=port,
            benchmark=args.benchmark,
            payload_key=args.payload,
            concurrency=concurrency,
            request_count=args.requests,
            warmup_count=args.warmup,
            id_gen=id_gen,
            timeout=args.timeout,
            verbose=True,
        )
        all_records.extend(records)
        all_summaries.append(summary)

    # --- Write output ---
    print(f"\n[3/3] Writing output...")
    write_request_csv(all_records, request_csv)
    write_summary_csv(all_summaries, summary_csv)
    print(f"       Requests CSV : {request_csv}")
    print(f"       Summary  CSV : {summary_csv}")

    # --- Summary table ---
    print()
    print("=" * 62)
    print("  RESULTS SUMMARY")
    print("=" * 62)
    print(
        f"  {'Concur':>6}  {'Done':>6}  {'OK':>6}  {'Err':>5}  "
        f"{'p50':>8}  {'p95':>8}  {'p99':>8}  {'Tput':>10}"
    )
    print("  " + "-" * 60)
    for s in all_summaries:
        print(
            f"  {s.concurrency:>6}  {s.completed:>6}  {s.success:>6}  {s.errors:>5}  "
            f"{s.p50_ms:>7.2f}ms  {s.p95_ms:>7.2f}ms  {s.p99_ms:>7.2f}ms  "
            f"{s.throughput_rps:>7.1f} r/s"
        )
    print("=" * 62)
    print(f"\nRun ID : {run_id}")
    print("Benchmark complete.\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
