# Architecture Decision Records (ADR) Index

This index catalogs all Architecture Decision Records (ADRs) established during the evolution of the Distributed Search Engine. Each record captures the context, options considered, decisions taken, and consequences for major architectural milestones.

All ADR files are located in the [`docs/decisions/`](file:///c:/Users/Daksh/Desktop/DseProj/docs/decisions) directory.

---

## Index of Architecture Decision Records

| ADR ID | Title | Status | Scope / Summary |
| :---: | :--- | :---: | :--- |
| **[ADR-001](decisions/ADR-001-tokenizer-design.md)** | **Tokenizer Design** | Accepted | Single-pass UTF-8 tokenization in C++20 with lowercase normalization and punctuation stripping. |
| **[ADR-002](decisions/ADR-002-inverted-index-design.md)** | **Inverted Index Design** | Accepted | In-memory hash-map index mapping terms to posting vectors `{doc_id, term_frequency}`. |
| **[ADR-003](decisions/ADR-003-query-processing-design.md)** | **Query Processing Design** | Accepted | Fast set intersection (AND) and union (OR) algorithms over sorted posting lists. |
| **[ADR-004](decisions/ADR-004-ranking-design.md)** | **TF-IDF Ranking Design** | Accepted | BM25/TF-IDF scoring implementation with global collection statistics. |
| **[ADR-005](decisions/ADR-005-search-api-design.md)** | **Search API Design** | Accepted | REST HTTP interface with `cpp-httplib` and `nlohmann/json` for query evaluation. |
| **[ADR-006](decisions/ADR-006-persistence-design.md)** | **Document Persistence Design** | Accepted | Append-only JSONL storage engine for document durability and crash recovery. |
| **[ADR-007](decisions/ADR-007-concurrency-design.md)** | **Concurrency and Thread Safety** | Accepted | Reader-writer locking (`std::shared_mutex`) and lock-free atomic counters for thread safety. |
| **[ADR-008](decisions/ADR-008-document-lifecycle-design.md)** | **Document Lifecycle (Update and Delete)** | Accepted | Atomic update-in-place and tombstone-free document removal semantics. |
| **[ADR-009](decisions/ADR-009-shard-architecture.md)** | **In-Process Shard Architecture** | Accepted | Modular shard partitioning dividing inverted index and storage by hash modulo. |
| **[ADR-010](decisions/ADR-010-node-abstraction.md)** | **Node Abstraction** | Accepted | `NodeClient` abstract interface decoupling coordination logic from transport. |
| **[ADR-011](decisions/ADR-011-remote-node-network.md)** | **Remote Node / Network Transport** | Accepted | HTTP/RPC transport (`RemoteNode` and `NodeServer`) for multi-process clustering. |
| **[ADR-012](decisions/ADR-012-failure-semantics.md)** | **Distributed Failure Semantics** | Accepted | Formalization of partial shard failures, error propagation, and degradation modes. |
| **[ADR-013](decisions/ADR-013-retry-resilience.md)** | **Retry and Resilience** | Accepted | Exponential backoff retry policy with full jitter to avoid thundering herd on network blips. |
| **[ADR-014](decisions/ADR-014-circuit-breaker.md)** | **Circuit Breaker Pattern** | Accepted | Three-state circuit breaker (`Closed`, `Open`, `HalfOpen`) preventing cascading failures. |
| **[ADR-015](decisions/ADR-015-phase-19f-distributed-reliability.md)** | **Phase 19F Distributed Reliability and Production Readiness** | Accepted | Production hardening, RAII consumer threads, loopback suppression, and crash safety. |
| **[ADR-016](decisions/ADR-016-synchronous-replication-authority.md)** | **Synchronous All-Replica Replication and Cluster Authority Model** | Accepted | Synchronous all-replica write replication in `ShardCoordinator` as the sole authority tier. |
| **[ADR-017](decisions/ADR-017-durable-outbox-event-pipeline.md)** | **Durable Outbox Event Pipeline** | Accepted | Durable `PersistentEventStore` decoupling document writes from Kafka stream publication. |
| **[ADR-018](decisions/ADR-018-unified-operational-observability.md)** | **Unified End-to-End Operational Observability** | Accepted | End-to-end `/metrics` telemetry combining search/write latencies, circuit breakers, and consumer lag. |
