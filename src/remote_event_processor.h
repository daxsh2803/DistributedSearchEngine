// Distributed Search Engine - Remote Event Processor (Phase 19E).
//
// Applies document mutations received from Kafka consumer events.
// This component bridges the gap between Kafka consumption and local
// shard/index state updates.
//
// Architecture:
//
//   KafkaConsumer (Phase 19D)
//       |
//       | handler callback (MessageHandler)
//       v
//   RemoteEventProcessor
//       |
//       | deserialize DocumentEvent
//       | validate shard ownership
//       | apply mutation locally
//       v
//   LocalNode::apply_remote_*()
//       |
//       v
//   Shard (DocumentStore + InvertedIndex)
//
// Feedback-loop prevention:
//   RemoteEventProcessor applies mutations through LocalNode's
//   apply_remote_*() methods, which directly mutate the Shard
//   WITHOUT publishing events or triggering replication.
//   This prevents the infinite loop:
//     Node A → Kafka → Node B → Kafka → Node A → ...
//
// Threading:
//   process() is safe for concurrent calls. Each call is self-contained.
//   The processor holds references to LocalNode instances but does not
//   own them — lifecycle is managed by the caller.
//
// Ownership:
//   RemoteEventProcessor does NOT own the LocalNode or ShardCoordinator.
//   All references must outlive the processor.

#pragma once

#include <atomic>
#include <cstddef>
#include <memory>
#include <string>
#include <unordered_map>

#include "inverted_index.h"  // for doc_id

namespace dse {

class LocalNode;

// ---------------------------------------------------------------------------
// RemoteEventProcessor
// ---------------------------------------------------------------------------

class RemoteEventProcessor {
public:
    // Statistics observable by callers.
    struct Stats {
        std::size_t events_received     = 0;
        std::size_t events_processed    = 0;
        std::size_t events_failed       = 0;
        std::size_t events_skipped      = 0;  // wrong shard or no-op
        std::size_t events_self_skipped = 0;  // source == own node
        std::size_t malformed_events    = 0;
        std::size_t index_operations    = 0;
        std::size_t update_operations   = 0;
        std::size_t remove_operations   = 0;
    };

    // Construct a processor with a map of node_id → LocalNode* and
    // the node_id of this processor instance (own_node_id).
    // The processor does NOT own the nodes. All pointers must outlive
    // the processor.
    //
    // own_node_id is used to skip events that originated from this
    // node (source_node_id == own_node_id), preventing self-delivery
    // feedback loops.
    RemoteEventProcessor(
        std::unordered_map<std::size_t, LocalNode*> nodes,
        std::size_t own_node_id);

    // Construct with default own_node_id = 0 (for tests/backwards compat).
    explicit RemoteEventProcessor(
        std::unordered_map<std::size_t, LocalNode*> nodes);

    // Process a raw JSON event payload from a specific Kafka topic.
    // Returns true if the event was successfully applied, false otherwise.
    //
    // This is the primary entry point for KafkaMessageBroker consumer
    // handlers. It handles deserialization, validation, shard routing,
    // and local mutation.
    bool process(const std::string& topic, const std::string& payload);

    // Own node ID — used for source_node_id enforcement.
    std::size_t own_node_id() const { return own_node_id_; }

    // Current statistics.
    Stats stats() const;

    // Outcome of processing an event.
    enum class Outcome {
        Applied,  // Successfully applied mutation to local shard
        Skipped,  // Acknowledged without mutation (self-event, wrong shard, or duplicate no-op)
        Failed    // Failed (malformed JSON, conflict, or mutation rejected)
    };

    // Reset statistics counters.
    void reset_stats();

private:
    // Process a document_indexed event.
    Outcome process_indexed(const std::string& payload);

    // Process a document_updated event.
    Outcome process_updated(const std::string& payload);

    // Process a document_removed event.
    Outcome process_removed(const std::string& payload);

    // Find a LocalNode that hosts the given shard.
    // Returns nullptr if no node hosts the shard.
    LocalNode* find_node_for_shard(std::size_t shard_id);

    // --- Node lookup ---
    std::unordered_map<std::size_t, LocalNode*> nodes_;

    // This node's identity. Events with source_node_id == own_node_id
    // are skipped (already applied locally).
    std::size_t own_node_id_;

    // Statistics (atomic for lock-free reads).
    std::atomic<std::size_t> events_received_{0};
    std::atomic<std::size_t> events_processed_{0};
    std::atomic<std::size_t> events_failed_{0};
    std::atomic<std::size_t> events_skipped_{0};
    std::atomic<std::size_t> events_self_skipped_{0};
    std::atomic<std::size_t> malformed_events_{0};
    std::atomic<std::size_t> index_operations_{0};
    std::atomic<std::size_t> update_operations_{0};
    std::atomic<std::size_t> remove_operations_{0};
};

} // namespace dse
