# Distributed Search Engine — AI Agent Instructions

## Project

This repository contains a distributed search engine implemented primarily in C++20.

The goal is to build a search engine from first principles and progressively evolve it into a scalable, fault-tolerant distributed system.

## Core Principle

The developer must understand the architecture and implementation.

Do not generate large amounts of code without explanation.

For every major component:

1. Explain the underlying concept.
2. Explain the proposed design.
3. Explain important trade-offs.
4. Explain time and space complexity.
5. Implement the component.
6. Add tests.
7. Build and run tests.
8. Report the results.

## Critical Restrictions

Do NOT use:

- Elasticsearch
- OpenSearch
- Apache Solr
- Lucene
- another complete search-engine implementation

The core search engine must be implemented ourselves.

Open-source infrastructure such as Redis, Kafka, PostgreSQL, Docker, Nginx, Prometheus and Grafana may be introduced only at the appropriate project phase.

## Development Methodology

Do not implement future phases prematurely.

Implement the current phase completely before moving to the next phase.

Keep the project buildable and testable after every phase.

Do not fabricate benchmark results.

Only report measurements that were actually obtained.

## Code Quality

Use modern C++20.

Prefer:

- RAII
- smart pointers
- const correctness
- STL
- clear ownership
- modular design
- meaningful names
- automated tests

Avoid unnecessary abstraction and over-engineering.

## Git

Do not modify unrelated files.

Do not reset or delete user changes.

Do not commit unless explicitly asked.

Before making major changes, inspect the current repository state.

## Current Project Phase

PHASE 1 — Text Processing and Tokenization

Phase 1A — Design and Learning: COMPLETE
Phase 1B — Implementation: COMPLETE

Phase 1 is fully implemented and verified.

Do not implement later search-engine functionality such as:
- inverted indexing
- query processing
- ranking
- persistence
- networking
- sharding
- replication
- distributed coordination

The next development phase is Phase 2 — Inverted Index.