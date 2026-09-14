# Phase 21 Learning: Benchmarking and Performance Characterization

## 1. Overview and Methodology

Phase 21 established an automated, reproducible benchmarking framework (harness scripts `bench_C.py` through `bench_I.py`) to systematically measure throughput, latency distributions ($P_{50}, P_{95}, P_{99}$), replication overhead, and asynchronous event propagation under varying client concurrency levels ($c=1, 2, 4, 8, 16$).

All measurements were obtained from reproducible local harness runs against live server processes.

---

## 2. Benchmark Summary Suite (A through J)

| Benchmark | Scope & Topology | Target Metric | Key Characterization |
| :--- | :--- | :--- | :--- |
| **Benchmark A** | 1 Node, Ingestion ($S=3$) | Write Throughput & Latency (Medium vs Small docs) | Measured raw single-node document indexing and inverted index segment writing. |
| **Benchmark B** | 1 Node, Search ($S=3$) | Read Throughput & Concurrency Scaling | Peak single-node throughput reached ~635 req/s at $c=4$ with $P_{50} = 6.68\text{ ms}$, scaling up to 1000 requests without errors. |
| **Benchmark C** | 3 Nodes, Ingestion ($S=3, R=1$) | Distributed Ingest Fan-out | Evaluated multi-node write throughput across network RPC endpoints. |
| **Benchmark D** | 3 Nodes, Search ($S=3, R=1$) | Distributed Cross-Shard Search | Measured cross-node scatter-gather latency with global TF-IDF calculation across 3 distinct node processes. |
| **Benchmark E** | 3 Nodes, Mixed ($S=3, R=1$) | 80% Read / 20% Write Mixed Workload | Verified concurrent read/write throughput without deadlocks or mutex starvation. |
| **Benchmark F** | 3 Nodes, Replication Overhead ($R=1$ vs $R=3$) | Synchronous Replication Latency Impact | Direct comparison of $R=1$ vs $R=3$ writes across $c \in [1, 16]$. At $c=1$, $P_{50}$ rose from 43.43ms ($R=1$) to 49.80ms ($R=3$) (+14.7% overhead). Under heavy concurrency ($c=16$), $P_{50}$ was 172.91ms for $R=3$. |
| **Benchmark G** | 3 Nodes + Kafka | Asynchronous Event Propagation Latency | Evaluated end-to-end latency from client write completion to Kafka receipt and consumer delivery report. |
| **Benchmark H** | 3 Nodes + Kafka | Kafka Consumer Lag Under Sustained Load | Monitored consumer lag accumulation during high-rate write bursts and recorded lag drain curves. |
| **Benchmark I** | 3 Nodes + Kafka | Resilience Under Kafka Broker Outage | Verified zero impact on client write operations during Kafka downtime; confirmed outbox queuing. |
| **Benchmark J** | Multi-Node + Kafka | **Observational Recovery Characterization** | Post-resilience observational study documenting state recovery, EventStore replay consistency, and lag resolution. |

---

## 3. Deep-Dive: Replication Overhead (Benchmark F)

Benchmark F measured the authoritative cost of Phase 17 Synchronous All-Replica Replication by evaluating 500 document writes across $R=1$ (single replica) versus $R=3$ (all 3 nodes write synchronously):

```
Concurrency (c) | R=1 P50 (ms) | R=3 P50 (ms) | P50 Overhead | R=1 Throughput | R=3 Throughput
----------------+--------------+--------------+--------------+----------------+---------------
c = 1           |    43.43 ms  |    49.80 ms  |   +14.68%    |   24.04 rps    |   20.12 rps
c = 2           |    46.76 ms  |    54.85 ms  |   +17.29%    |   42.16 rps    |   35.77 rps
c = 4           |    51.17 ms  |    87.65 ms  |   +71.28%    |   74.35 rps    |   43.28 rps
c = 8           |    70.46 ms  |   164.01 ms  |  +132.77%    |  109.92 rps    |   45.55 rps
c = 16          |    72.34 ms  |   172.91 ms  |  +139.02%    |  107.96 rps    |   44.45 rps
```

### Insights:
- At low concurrency ($c=1, 2$), parallel RPC fanout via `std::async` kept synchronous replication latency overhead below 18%.
- At higher concurrency ($c=8, 16$), socket and lock contention across the 3 physical server processes increased $P_{50}$ to ~172ms, stabilizing throughput at ~45 rps for $R=3$ versus ~108 rps for $R=1$.

---

## 4. Benchmark J: Observational Recovery Analysis

Benchmark J was executed strictly as an **observational** benchmark rather than an automated pass/fail test. Its purpose was to inspect:
1. **EventStore State:** Verified that events generated during simulated disconnections transitioned to `PENDING` on reboot without orphan records.
2. **Kafka Consumer Offset Catch-up:** Recorded that after broker restoration, consumer groups caught up to zero lag sequentially.
3. **Replica Consistency:** Validated that shard document counts across all replicas matched the primary node exactly after recovery.

---

## 5. Summary of Key Findings

1. **Synchronous Replication Bounds Throughput by Design:** Requiring all replicas to confirm writes guarantees strict consistency but establishes a throughput ceiling under high concurrency.
2. **Kafka Decoupling Operates as Intended:** Benchmarks G, H, and I demonstrated that asynchronous propagation latency runs independently of client write response times, confirming the stability of the two-tier architecture.
