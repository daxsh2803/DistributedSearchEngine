# Phase 19E — Distributed Event Processing

## Objective

Make Kafka consumer groups perform useful distributed document-mutation
processing. When Node A mutates a document, the event flows through Kafka
and a consumer on Node B can apply the mutation to its local shard/index.

## Why Kafka Consumers Are Needed

Phase 17 implements synchronous write-all replication: the coordinator
fans out mutations to ALL replicas before returning success. This is
correct for strong consistency but means every replica must be reachable.

Phase 19E adds **asynchronous event propagation** through Kafka. This
enables:

- Event notification for downstream services
- Asynchronous replication as a future option
- Cross-node event processing for distributed search

## Architecture

```
Node A (originating, node_id=A)
    |
    | ShardCoordinator::ingest()
    v
Local mutation on primary shard
    |
    | ShardCoordinator publishes ONE DocumentEvent with
    | source_node_id = A and document_content
    v
EventDispatcher → KafkaMessageBroker (producer)
    |
    v
Kafka Broker (topics: documents.indexed, documents.updated, documents.removed)
    |
    | Each node has its own consumer group: dse-node-<node_id>
    | Each node receives the full event stream independently
    v
KafkaConsumer (per-node consumer group)
    |
    | MessageHandler callback → RemoteEventProcessor::process()
    v
RemoteEventProcessor
    |
    | 1. Deserialize DocumentEvent
    | 2. If source_node_id == own_node_id: skip (self-event)
    | 3. Find node hosting the shard
    | 4. If no node hosts shard: skip (wrong shard)
    | 5. Apply via LocalNode::apply_remote_*()
    |    - Idempotent INDEX: if doc exists with same content → no-op
    |    - Idempotent UPDATE: if content matches → no-op
    |    - Idempotent REMOVE: if doc absent → no-op
    | 6. Persist via Shard::save() (no event publication)
    v
Shard (DocumentStore + InvertedIndex)
```

## RemoteEventProcessor

A narrowly scoped component responsible for:

1. Receiving a raw Kafka message payload
2. Deserializing the DocumentEvent
3. Validating shard ownership
4. Enforcing self-event skipping (source_node_id check)
5. Applying the mutation to the correct local shard

### Three-Way Processing Semantics

The processor evaluates each incoming event into one of three distinct outcomes:

1. **Applied**:
   - The mutation genuinely modified the local shard state (added new doc, updated doc with differing content, removed existing doc).
   - Increments `events_processed_` and the operation-specific counter (`index_operations_`, `update_operations_`, `remove_operations_`).
   - Persists state via `shard->save()`.
   - Returns `true` (Kafka offset committed).

2. **Skipped**:
   - The event is valid and successfully processed, but required no local mutation:
     - **Self-originated event**: `source_node_id == own_node_id`. Increments `events_skipped_` and `events_self_skipped_`.
     - **Unhosted shard**: Local node does not host the target shard. Increments `events_skipped_`.
     - **Idempotent duplicates**:
       - Duplicate INDEX with matching content.
       - Duplicate UPDATE with matching content.
       - Duplicate REMOVE on an already absent document.
       All increment `events_skipped_` (and `index_operations_` / `update_operations_` / `remove_operations_` are NOT incremented).
   - Returns `true` (Kafka offset committed; no infinite redelivery).

3. **Failed**:
   - Processing encountered a failure condition requiring retry:
     - **Malformed JSON payload**: Could not deserialize event. Increments `malformed_events_`.
     - **Content conflict**: Duplicate INDEX with different content. Increments `events_failed_`.
     - **Missing document for UPDATE**: UPDATE event received for a non-existent document. Increments `events_failed_`.
   - Returns `false` (Kafka offset NOT committed; message eligible for redelivery).

It does NOT:
- Publish events (prevents feedback loops)
- Trigger replication
- Manage Kafka offsets directly (the KafkaConsumer driver commits based on the boolean return value)

It DOES:
- Persist local state via `Shard::save()` after successful applied mutations
  (safe: save() does not publish events or trigger replication)
- Skip self-events (source_node_id == own_node_id)
- Distinguish applied mutations from skipped idempotent no-ops in statistics counters.

## Local vs Remote Mutation

| Operation | Local Path | Remote Path |
|-----------|-----------|-------------|
| Add | `ShardCoordinator::ingest()` | `RemoteEventProcessor::process()` |
| Update | `ShardCoordinator::update()` | `RemoteEventProcessor::process()` |
| Remove | `ShardCoordinator::remove()` | `RemoteEventProcessor::process()` |

The local path goes through NodeClient → Shard and publishes events.
The remote path goes through LocalNode::apply_remote_*() → Shard
and does NOT publish events.

## Feedback-Loop Prevention

RemoteEventProcessor applies mutations through `LocalNode::apply_remote_*()`,
which call `Shard::add_document()`, `Shard::update_document()`, or
`Shard::remove_document()` directly. These methods:

- Do NOT publish DocumentEvents
- Do NOT trigger Phase 17 replication

This structurally prevents the infinite loop:

```
Node A → Kafka → Node B → local mutation → Kafka → Node A → ...
```

Remote mutations DO persist to disk via `Shard::save()` after a successful
mutation. This is safe because `save()` writes the document store to a
local JSONL file and does not publish events or trigger replication.

Persistence is important because: if a node applies a remote event and
commits the Kafka offset, then crashes before the next checkpoint, the
event would be lost without local persistence. With `save()`, the mutation
survives a restart. Kafka will not replay events whose offsets have been
committed, so local persistence is the durability guarantee for applied
remote mutations.

## Consumer Groups (Node-Specific)

Each KafkaMessageBroker instance belongs to one consumer group. For
distributed deployments, **each node must have its own consumer group**
so that every node receives the full event stream independently.

Recommended configuration:

```
dse-node-<node_id>
```

For example:

- Node A (node_id=0) → group `dse-node-0`
- Node B (node_id=1) → group `dse-node-1`

Because each node has an independent consumer group, an event for a shard
that this node does not host can be safely acknowledged/skipped by the
RemoteEventProcessor without affecting other nodes. Kafka delivers the
full event stream to each group independently.

If `KafkaBrokerConfig::group_id` is not explicitly set, the application
should derive a node-specific group ID. The explicit `group_id` field
remains available for override if needed.

## Partitioning / Key Strategy

Events are currently published with `document_id` as the Kafka message
key. This ensures all events for the same document land on the same
partition, preserving per-document ordering.

## Ordering Limitations

- **Per-document ordering**: Guaranteed within a single partition.
- **Cross-document ordering**: NOT guaranteed across partitions.
- **Cross-topic ordering**: NOT guaranteed between indexed/updated/removed.

The system relies on idempotent operations rather than strict ordering
for correctness.

## At-Least-Once Semantics

Kafka provides at-least-once delivery. The RemoteEventProcessor is
designed to handle duplicate delivery without infinite redelivery:

- **Indexed (duplicate, same content)**: Returns true (idempotent no-op).
  Offset committed. No redelivery.
- **Indexed (duplicate, different content)**: Returns false (content
  conflict). Offset NOT committed. Kafka redelivers.
- **Updated (duplicate, same content)**: Returns true (idempotent no-op).
- **Updated (missing document)**: Returns false. Kafka redelivers.
- **Removed (duplicate or already absent)**: Returns true (idempotent
  no-op). Offset committed.

The critical fix: duplicate INDEX and REMOVE no longer return false.
Previously, a duplicate INDEX returned false (because `add_document`
rejected the existing ID), and a duplicate REMOVE returned false (because
the document was absent). Both caused the Kafka consumer to NOT commit
the offset, leading to infinite redelivery across restarts. Now both are
idempotent successes.

## Idempotency

The system provides natural idempotency through the Shard API and the
RemoteEventProcessor's handling of return values:

- **INDEX (duplicate, same content)**: `apply_remote_indexed()` detects
  the document already exists with identical content and returns true
  (idempotent no-op). The Kafka offset is committed; no redelivery.
- **INDEX (duplicate, different content)**: Treated as a content conflict.
  Returns false. Kafka redelivers. The caller must resolve the conflict.
- **UPDATE (duplicate, same content)**: Detected via content comparison;
  returns true (idempotent no-op).
- **UPDATE (missing document)**: Returns false. Kafka redelivers.
- **REMOVE (duplicate or already absent)**: Returns true
  (idempotent no-op). An absent document is treated as success regardless
  of whether the document was never created or was already removed.

This means a REMOVE that arrives before its corresponding INDEX is also
acknowledged. The subsequent INDEX will recreate the document. This is the
standard idempotent-consumer approach for delete events under at-least-once
delivery.

No event-ID cache is used. No versioning is implemented.

Future work: Document versioning would enable rejecting stale updates.

## Failure / Recovery

- **Malformed event**: Parser returns false → offset NOT committed →
  Kafka redelivers.
- **Wrong shard**: Processor returns true → offset committed →
  no redelivery needed.
- **Self-event (source == own node)**: Skipped → true → offset committed.
- **Shard mutation fails (genuine conflict)**: Processor returns false →
  offset NOT committed → Kafka redelivers.
- **Node not found**: Event skipped → offset committed.

Note: the previous behavior where a duplicate INDEX returned false
(causing infinite redelivery) has been fixed. Duplicate INDEX with the
same content now returns true. Duplicate REMOVE on an absent document now
returns true.

## Node Identity and Self-Event Skipping

Each LocalNode has a stable `node_id` set at construction time.
Events include `source_node_id` to identify the originating node.
This metadata is preserved through serialization/deserialization.

### source_node_id semantics

The ShardCoordinator sets `source_node_id` to the **primary node for the
affected shard** (via `node_for_shard(shard_id).node_id()`), not to
`nodes_[0]->node_id()` (which was merely the first node in the vector).
This means the event carries the ID of the node that owns the shard and
performed the mutation.

### Self-event skipping

The RemoteEventProcessor is constructed with an `own_node_id`. When
processing an event, if `event.source_node_id == own_node_id`, the event
is skipped (return true, commit offset, no mutation). This prevents
self-delivery feedback loops:

- Node A publishes an event with `source_node_id = A`.
- Node A's own consumer receives the event.
- The RemoteEventProcessor on Node A sees `source == own` and skips it.
- No duplicate mutation occurs.

Each node still receives the full event stream (because each node has its
own consumer group), but self-events are no-ops.

## Relationship to Phase 17 Replication

Phase 17 synchronous replication remains the primary consistency
mechanism. Kafka event processing is an additional asynchronous
propagation path that runs alongside, not instead of, Phase 17.

## Testing

Tests cover:

- Indexed/updated/removed event processing
- Feedback-loop prevention
- Idempotent duplicate INDEX (same content → success)
- Idempotent duplicate UPDATE (same content → success)
- Idempotent duplicate REMOVE (absent → success)
- REMOVE-before-INDEX (absent → success, no-op)
- UPDATE-before-INDEX (missing → failure, retry)
- Content conflict on INDEX (different content → failure)
- Wrong-shard handling
- Malformed event handling
- Source node metadata
- Source node enforcement (self-event skipping)
- Statistics tracking (including self_skipped counter)
- Multiple shards across nodes
- Concurrent processing
- Event ID stability
- R=2 single logical event

## Limitations

1. **No document versioning**: Stale updates can overwrite newer ones.
   This is documented as future work.
2. **No cross-topic ordering**: Indexed/updated/removed events on
   different topics have no ordering guarantee.
3. **No persistent retry**: Failed events rely on Kafka's offset
   mechanism. Process crashes lose uncommitted offsets.
4. **REMOVE-before-INDEX accepted**: A REMOVE for a document that has
   not yet been indexed is treated as a successful no-op. The subsequent
   INDEX will recreate the document. This is the standard idempotent-
   consumer trade-off for delete events under at-least-once delivery.
5. **In-memory only**: Events are lost on process restart (EventStore is
   in-memory). Remote mutations persist via Shard::save() but only if
   the shard has a persistence path configured.
6. **No consumer lag monitoring**: Deferred to a future phase.

## Relationship to Phase 17 Replication

Phase 17 synchronous write-all replication remains the primary consistency
mechanism. Kafka event processing is an additional asynchronous
propagation path that runs alongside, not instead of, Phase 17.
Phase 17 semantics are unchanged: the coordinator fans out to all replicas
synchronously, and each replica's LocalNode write path (add_document,
update_document, remove_document) publishes events and persists state.

Remote mutations via RemoteEventProcessor bypass event publication and
Phase 17 replication by design. They only affect the local shard and
persist locally.

## Future Work

- Document versioning for stale-event rejection
- Kafka DLQ topics for permanently failed events
- Consumer lag monitoring
- Event sourcing / CQRS
- Schema registry for event validation
