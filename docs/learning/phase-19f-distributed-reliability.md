# Phase 19F: Distributed Event Reliability

This phase hardens the Kafka-based asynchronous event processing pipeline, transforming it from a "best-effort" message bus into a highly reliable, durable log suitable for production distributed systems.

## The Challenge

In Phase 19E, we implemented a basic message broker integration. However, that implementation lacked strict reliability guarantees:
- Events were fire-and-forget. If the broker was down, events were lost from memory.
- If a consumer failed to process an event (e.g., due to local disk full, transient lock), the offset was simply skipped or not committed, which caused problems if the process didn't crash because Kafka won't redeliver it during the same session.
- We used multiple topics (`documents.indexed`, etc.), meaning events for the *same document* might arrive out of order depending on partition assignments and topic-level timing.

## The Solution

Phase 19F introduced several structural changes to address these gaps:

### 1. The Single Topic Pattern
We consolidated all document mutations into a single `documents.mutations` topic.
- **Why?** Kafka guarantees ordering only within a single partition of a single topic. By using the `document_id` as the message key on a single topic, we ensure that an `INDEX`, followed by an `UPDATE`, followed by a `REMOVE` for the same document are placed in the same partition and consumed in exactly that order.

### 2. Strict Sequential Retry
Consumer handlers no longer just return `false` on failure and let the loop continue.
- **Implementation:** The `KafkaMessageBroker` loops on a failed message internally, sleeping with exponential backoff until the handler succeeds.
- **Result:** We never advance the Kafka offset past a failing message. This guarantees at-least-once ordered delivery, at the risk of head-of-line blocking if a message is a "poison pill" (permanently unprocessable).

### 3. Delivery Reports & EventStore
We now fully close the loop on producer reliability.
- **Implementation:** When `EventDispatcher` sends a message to Kafka, it registers a delivery callback. The local `EventStore` holds the message in `DISPATCHING` state until the callback fires. If the callback reports success, it transitions to `PUBLISHED`. If it reports failure, it transitions to `FAILED` and can be retried.

### 4. Durability Invariant
`RemoteEventProcessor` synchronously calls `Shard::save()` when applying mutations before it returns `true`. Only when it returns `true` does the consumer commit the Kafka offset. This guarantees that no data is acknowledged until it is durably on disk.
