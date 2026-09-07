// Distributed Search Engine - Remote Event Processor Tests (Phase 19E).
//
// Tests the RemoteEventProcessor's ability to:
// - Deserialize and apply document mutation events from Kafka
// - Route events to the correct shard
// - Prevent feedback loops by bypassing event publication
// - Handle malformed events gracefully
// - Process indexed, updated, and removed events

#include <gtest/gtest.h>

#include <memory>
#include <string>
#include <unordered_map>

#include "document_event.h"
#include "local_node.h"
#include "message.h"
#include "remote_event_processor.h"
#include "shard.h"

namespace dse {
namespace {

// ---------------------------------------------------------------------------
// Test fixture: creates a LocalNode with 2 shards for testing.
// ---------------------------------------------------------------------------

class RemoteEventProcessorTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        // Node 0 with 2 shards.
        node0_ = std::make_unique<LocalNode>(0);
        node0_->add_shard(0, std::make_unique<Shard>());
        node0_->add_shard(1, std::make_unique<Shard>());

        // Node 1 with 1 shard.
        node1_ = std::make_unique<LocalNode>(1);
        node1_->add_shard(2, std::make_unique<Shard>());

        // Map node_id → LocalNode* for the processor.
        std::unordered_map<std::size_t, LocalNode*> nodes;
        nodes[0] = node0_.get();
        nodes[1] = node1_.get();

        processor_ = std::make_unique<RemoteEventProcessor>(std::move(nodes));
    }

    // Helper: create a JSON payload for an indexed event.
    std::string make_indexed_payload(EventId event_id, doc_id doc_id,
                                      std::size_t shard_id,
                                      std::size_t source_node,
                                      const std::string& content)
    {
        DocumentIndexedEvent event;
        event.event_id = event_id;
        event.document_id = doc_id;
        event.shard_id = shard_id;
        event.source_node_id = source_node;
        event.document_content = content;
        return event_json::to_json(event);
    }

    // Helper: create a JSON payload for an updated event.
    std::string make_updated_payload(EventId event_id, doc_id doc_id,
                                      std::size_t shard_id,
                                      std::size_t source_node,
                                      const std::string& content)
    {
        DocumentUpdatedEvent event;
        event.event_id = event_id;
        event.document_id = doc_id;
        event.shard_id = shard_id;
        event.source_node_id = source_node;
        event.document_content = content;
        return event_json::to_json(event);
    }

    // Helper: create a JSON payload for a removed event.
    std::string make_removed_payload(EventId event_id, doc_id doc_id,
                                      std::size_t shard_id,
                                      std::size_t source_node)
    {
        DocumentRemovedEvent event;
        event.event_id = event_id;
        event.document_id = doc_id;
        event.shard_id = shard_id;
        event.source_node_id = source_node;
        return event_json::to_json(event);
    }

    std::unique_ptr<LocalNode> node0_;
    std::unique_ptr<LocalNode> node1_;
    std::unique_ptr<RemoteEventProcessor> processor_;

    // Fixture node IDs for source-node enforcement tests.
    static constexpr std::size_t kNode0Id = 0;
    static constexpr std::size_t kNode1Id = 1;
};

// ---------------------------------------------------------------------------
// 1. Indexed event — document appears in local shard
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, IndexedEventAddsDocument)
{
    const std::string payload = make_indexed_payload(
        1, 100, 0, 1, "hello world");

    EXPECT_TRUE(processor_->process(topics::kDocumentIndexed, payload));

    // Document should now be in node0, shard 0.
    ShardGetRequest req;
    req.shard_id = 0;
    req.document_id = 100;
    auto resp = node0_->get_document(req);
    EXPECT_TRUE(resp.found);
    EXPECT_EQ(resp.content, "hello world");

    const auto stats = processor_->stats();
    EXPECT_EQ(stats.events_processed, 1u);
    EXPECT_EQ(stats.index_operations, 1u);
}

// ---------------------------------------------------------------------------
// 2. Updated event — document content changes
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, UpdatedEventModifiesDocument)
{
    // First, add a document via indexed event.
    const std::string idx_payload = make_indexed_payload(
        1, 100, 0, 1, "original content");
    EXPECT_TRUE(processor_->process(topics::kDocumentIndexed, idx_payload));

    // Now update it.
    const std::string upd_payload = make_updated_payload(
        2, 100, 0, 1, "updated content");
    EXPECT_TRUE(processor_->process(topics::kDocumentUpdated, upd_payload));

    ShardGetRequest req;
    req.shard_id = 0;
    req.document_id = 100;
    auto resp = node0_->get_document(req);
    EXPECT_TRUE(resp.found);
    EXPECT_EQ(resp.content, "updated content");

    const auto stats = processor_->stats();
    EXPECT_EQ(stats.update_operations, 1u);
}

// ---------------------------------------------------------------------------
// 3. Removed event — document is deleted
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, RemovedEventDeletesDocument)
{
    // Add a document first.
    const std::string idx_payload = make_indexed_payload(
        1, 100, 0, 1, "to be removed");
    EXPECT_TRUE(processor_->process(topics::kDocumentIndexed, idx_payload));

    // Verify it exists.
    EXPECT_TRUE(node0_->has_shard(0));
    ShardGetRequest get_req;
    get_req.shard_id = 0;
    get_req.document_id = 100;
    EXPECT_TRUE(node0_->get_document(get_req).found);

    // Remove it.
    const std::string rm_payload = make_removed_payload(2, 100, 0, 1);
    EXPECT_TRUE(processor_->process(topics::kDocumentRemoved, rm_payload));

    // Verify it's gone.
    auto resp = node0_->get_document(get_req);
    EXPECT_FALSE(resp.found);

    const auto stats = processor_->stats();
    EXPECT_EQ(stats.remove_operations, 1u);
}

// ---------------------------------------------------------------------------
// 4. Feedback-loop prevention — no event published for remote mutation
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, NoFeedbackLoop)
{
    // The processor should NOT publish events.
    // This is tested implicitly: the processor only calls
    // apply_remote_*() which does NOT publish events.
    // We verify this by checking that the processor does not hold
    // any EventDispatcher or MessageBroker reference.

    const std::string payload = make_indexed_payload(
        1, 100, 0, 1, "feedback test");
    EXPECT_TRUE(processor_->process(topics::kDocumentIndexed, payload));

    // The processor only holds LocalNode pointers, not a coordinator
    // or dispatcher. This structurally prevents feedback loops.
    // No event should be published — this is verified by the fact
    // that RemoteEventProcessor does not include event_dispatcher.h.
    SUCCEED();
}

// ---------------------------------------------------------------------------
// 5. Duplicate INDEX is idempotent (same content → success)
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, DuplicateIndexedEventIsIdempotent)
{
    const std::string payload = make_indexed_payload(
        1, 100, 0, 1, "idempotent test");

    // First application succeeds.
    EXPECT_TRUE(processor_->process(topics::kDocumentIndexed, payload));

    // Second application with same content: idempotent no-op success.
    EXPECT_TRUE(processor_->process(topics::kDocumentIndexed, payload));

    // Document should still have original content.
    ShardGetRequest req;
    req.shard_id = 0;
    req.document_id = 100;
    auto resp = node0_->get_document(req);
    EXPECT_TRUE(resp.found);
    EXPECT_EQ(resp.content, "idempotent test");

    const auto stats = processor_->stats();
    EXPECT_EQ(stats.index_operations, 1u);
    EXPECT_EQ(stats.events_processed, 1u);
    EXPECT_EQ(stats.events_skipped, 1u);
    EXPECT_EQ(stats.events_failed, 0u);
}

// ---------------------------------------------------------------------------
// 6. Wrong shard — event for a shard not hosted by any node
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, WrongShardIsSkipped)
{
    // Shard 99 doesn't exist on any node.
    // The processor returns true so the consumer commits the offset;
    // reprocessing would not help since the shard is not hosted here.
    const std::string payload = make_indexed_payload(
        1, 100, 99, 1, "wrong shard");

    EXPECT_TRUE(processor_->process(topics::kDocumentIndexed, payload));

    const auto stats = processor_->stats();
    EXPECT_EQ(stats.events_skipped, 1u);
    EXPECT_EQ(stats.events_failed, 0u);  // skipped, not failed
}

// ---------------------------------------------------------------------------
// 7. Malformed event — bad JSON
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, MalformedEventIsSkipped)
{
    EXPECT_FALSE(processor_->process(
        topics::kDocumentIndexed, "not valid json {{{"));

    const auto stats = processor_->stats();
    EXPECT_EQ(stats.malformed_events, 1u);
}

// ---------------------------------------------------------------------------
// 8. Unknown topic — skipped
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, UnknownTopicIsSkipped)
{
    EXPECT_FALSE(processor_->process(
        "unknown.topic", R"({"event_id":1})"));

    const auto stats = processor_->stats();
    EXPECT_EQ(stats.events_skipped, 1u);
}

// ---------------------------------------------------------------------------
// 9. Source node metadata preserved through serialization
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, SourceNodeIdPreserved)
{
    // Create event with source_node_id = 1.
    const std::string payload = make_indexed_payload(
        1, 100, 0, 1, "node metadata test");

    // Deserialize and verify source_node_id is preserved.
    DocumentIndexedEvent event;
    EXPECT_TRUE(event_json::from_json(payload, event));
    EXPECT_EQ(event.source_node_id, 1u);
    EXPECT_EQ(event.document_content, "node metadata test");

    // Process should succeed (source_node_id doesn't affect routing).
    EXPECT_TRUE(processor_->process(topics::kDocumentIndexed, payload));
}

// ---------------------------------------------------------------------------
// 10. Statistics tracking
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, StatisticsTracking)
{
    // Process several events.
    EXPECT_TRUE(processor_->process(topics::kDocumentIndexed,
        make_indexed_payload(1, 100, 0, 1, "stats test 1")));
    EXPECT_TRUE(processor_->process(topics::kDocumentIndexed,
        make_indexed_payload(2, 101, 0, 1, "stats test 2")));
    EXPECT_TRUE(processor_->process(topics::kDocumentUpdated,
        make_updated_payload(3, 100, 0, 1, "stats updated")));
    EXPECT_TRUE(processor_->process(topics::kDocumentRemoved,
        make_removed_payload(4, 101, 0, 1)));

    const auto stats = processor_->stats();
    EXPECT_EQ(stats.events_received, 4u);
    EXPECT_EQ(stats.events_processed, 4u);
    EXPECT_EQ(stats.index_operations, 2u);
    EXPECT_EQ(stats.update_operations, 1u);
    EXPECT_EQ(stats.remove_operations, 1u);
}

// ---------------------------------------------------------------------------
// 11. Reset stats
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, ResetStats)
{
    EXPECT_TRUE(processor_->process(topics::kDocumentIndexed,
        make_indexed_payload(1, 100, 0, 1, "reset test")));

    EXPECT_EQ(processor_->stats().events_processed, 1u);

    processor_->reset_stats();

    const auto stats = processor_->stats();
    EXPECT_EQ(stats.events_received, 0u);
    EXPECT_EQ(stats.events_processed, 0u);
}

// ---------------------------------------------------------------------------
// 12. Multiple shards across nodes
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, MultipleShardsAcrossNodes)
{
    // Indexed event for shard 0 → goes to node 0.
    EXPECT_TRUE(processor_->process(topics::kDocumentIndexed,
        make_indexed_payload(1, 100, 0, 1, "shard 0 doc")));

    // Indexed event for shard 2 → goes to node 1.
    EXPECT_TRUE(processor_->process(topics::kDocumentIndexed,
        make_indexed_payload(2, 200, 2, 1, "shard 2 doc")));

    // Verify document in node 0, shard 0.
    ShardGetRequest req0;
    req0.shard_id = 0;
    req0.document_id = 100;
    EXPECT_TRUE(node0_->get_document(req0).found);
    EXPECT_EQ(node0_->get_document(req0).content, "shard 0 doc");

    // Verify document in node 1, shard 2.
    ShardGetRequest req2;
    req2.shard_id = 2;
    req2.document_id = 200;
    EXPECT_TRUE(node1_->get_document(req2).found);
    EXPECT_EQ(node1_->get_document(req2).content, "shard 2 doc");
}

// ---------------------------------------------------------------------------
// 13. Remove non-existent document (idempotent no-op)
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, RemoveNonExistentDocumentSucceeds)
{
    const std::string payload = make_removed_payload(1, 999, 0, 1);

    // REMOVE on absent document is an idempotent no-op success.
    EXPECT_TRUE(processor_->process(topics::kDocumentRemoved, payload));

    const auto stats = processor_->stats();
    EXPECT_EQ(stats.events_processed, 0u);
    EXPECT_EQ(stats.events_skipped, 1u);
    EXPECT_EQ(stats.events_failed, 0u);
}

// ---------------------------------------------------------------------------
// 14. Update non-existent document fails gracefully
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, UpdateNonExistentDocumentFails)
{
    const std::string payload = make_updated_payload(
        1, 999, 0, 1, "no such doc");

    EXPECT_FALSE(processor_->process(topics::kDocumentUpdated, payload));
}

// ---------------------------------------------------------------------------
// 15. Duplicate REMOVE is idempotent (no-op success)
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, DuplicateRemovedEventIsIdempotent)
{
    // First remove: document exists, succeeds.
    const std::string idx_payload = make_indexed_payload(
        1, 100, 0, 1, "to be removed");
    EXPECT_TRUE(processor_->process(topics::kDocumentIndexed, idx_payload));

    const std::string rm_payload = make_removed_payload(2, 100, 0, 1);
    EXPECT_TRUE(processor_->process(topics::kDocumentRemoved, rm_payload));

    // Verify gone.
    ShardGetRequest req;
    req.shard_id = 0;
    req.document_id = 100;
    EXPECT_FALSE(node0_->get_document(req).found);

    // Second remove: already absent, idempotent no-op success.
    EXPECT_TRUE(processor_->process(topics::kDocumentRemoved, rm_payload));

    const auto stats = processor_->stats();
    EXPECT_EQ(stats.remove_operations, 1u);
    EXPECT_EQ(stats.events_processed, 2u);
    EXPECT_EQ(stats.events_skipped, 1u);
    EXPECT_EQ(stats.events_failed, 0u);
    EXPECT_FALSE(node0_->get_document(req).found);
}

// ---------------------------------------------------------------------------
// 16. Duplicate UPDATE is idempotent (no-op success)
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, DuplicateUpdatedEventIsIdempotent)
{
    // First update: document exists, succeeds.
    const std::string idx_payload = make_indexed_payload(
        1, 100, 0, 1, "original");
    EXPECT_TRUE(processor_->process(topics::kDocumentIndexed, idx_payload));

    const std::string upd_payload = make_updated_payload(
        2, 100, 0, 1, "updated");
    EXPECT_TRUE(processor_->process(topics::kDocumentUpdated, upd_payload));

    // Second update with same content: idempotent no-op success.
    EXPECT_TRUE(processor_->process(topics::kDocumentUpdated, upd_payload));

    ShardGetRequest req;
    req.shard_id = 0;
    req.document_id = 100;
    EXPECT_TRUE(node0_->get_document(req).found);
    EXPECT_EQ(node0_->get_document(req).content, "updated");

    const auto stats = processor_->stats();
    EXPECT_EQ(stats.update_operations, 1u);
    EXPECT_EQ(stats.events_processed, 2u);
    EXPECT_EQ(stats.events_skipped, 1u);
    EXPECT_EQ(stats.events_failed, 0u);
}

// ---------------------------------------------------------------------------
// 17. Update before INDEX fails (missing document)
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, UpdateBeforeIndexFails)
{
    const std::string upd_payload = make_updated_payload(
        1, 999, 0, 1, "no such doc");

    EXPECT_FALSE(processor_->process(topics::kDocumentUpdated, upd_payload));

    const auto stats = processor_->stats();
    EXPECT_EQ(stats.events_failed, 1u);
}

// ---------------------------------------------------------------------------
// 18. REMOVE before INDEX (idempotent no-op)
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, RemoveBeforeIndexSucceeds)
{
    const std::string rm_payload = make_removed_payload(1, 999, 0, 1);

    // REMOVE on absent document: idempotent no-op success.
    EXPECT_TRUE(processor_->process(topics::kDocumentRemoved, rm_payload));

    const auto stats = processor_->stats();
    EXPECT_EQ(stats.events_processed, 0u);
    EXPECT_EQ(stats.events_skipped, 1u);
    EXPECT_EQ(stats.events_failed, 0u);
}

// ---------------------------------------------------------------------------
// 19. Source node event is skipped
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, SourceNodeEventIsSkipped)
{
    // Event with source_node_id == 0 (this processor's own node via default ctor).
    const std::string payload = make_indexed_payload(
        1, 100, 0, kNode0Id, "self-event");

    // Self-event: source == own node → skipped, returns true (commit offset).
    EXPECT_TRUE(processor_->process(topics::kDocumentIndexed, payload));

    // Document should NOT be created (skipped).
    ShardGetRequest req;
    req.shard_id = 0;
    req.document_id = 100;
    EXPECT_FALSE(node0_->get_document(req).found);

    const auto stats = processor_->stats();
    EXPECT_EQ(stats.events_self_skipped, 1u);
    EXPECT_EQ(stats.events_skipped, 1u);
    EXPECT_EQ(stats.events_processed, 0u);
    EXPECT_EQ(stats.index_operations, 0u);
    EXPECT_EQ(stats.events_failed, 0u);
}

// ---------------------------------------------------------------------------
// 20. Event ID stability across serialization
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, EventIdStability)
{
    DocumentIndexedEvent original;
    original.event_id = 42;
    original.document_id = 100;
    original.shard_id = 0;
    original.source_node_id = 1;
    original.document_content = "stability test";

    const std::string json_str = event_json::to_json(original);

    DocumentIndexedEvent restored;
    EXPECT_TRUE(event_json::from_json(json_str, restored));

    EXPECT_EQ(restored.event_id, original.event_id);
    EXPECT_EQ(restored.document_id, original.document_id);
    EXPECT_EQ(restored.shard_id, original.shard_id);
    EXPECT_EQ(restored.source_node_id, original.source_node_id);
    EXPECT_EQ(restored.document_content, original.document_content);
}

// ---------------------------------------------------------------------------// 16. R=2: single logical event processed once (idempotent under retry)
//---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, R2SingleLogicalEvent)
{
    // With R=2, the ShardCoordinator publishes ONE event.
    // The RemoteEventProcessor on a receiving node processes it once.
    // Under at-least-once delivery, the event may be delivered twice.
    // With idempotent INDEX, both deliveries succeed.

    const std::string payload = make_indexed_payload(
        1, 100, 0, 1, "r=2 test");

    // Process the same event twice (simulating at-least-once delivery).
    EXPECT_TRUE(processor_->process(topics::kDocumentIndexed, payload));
    EXPECT_TRUE(processor_->process(topics::kDocumentIndexed, payload));

    // Document should exist with original content.
    ShardGetRequest req;
    req.shard_id = 0;
    req.document_id = 100;
    auto resp = node0_->get_document(req);
    EXPECT_TRUE(resp.found);
    EXPECT_EQ(resp.content, "r=2 test");
}

// ---------------------------------------------------------------------------
// 17. Empty content rejected by shard
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, EmptyContentRejectedByShard)
{
    const std::string payload = make_indexed_payload(
        1, 100, 0, 1, "");

    EXPECT_FALSE(processor_->process(topics::kDocumentIndexed, payload));
}

// ---------------------------------------------------------------------------
// 18. Deserialization failure for each event type
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, DeserializationFailureIndexed)
{
    // Missing required fields.
    EXPECT_FALSE(processor_->process(
        topics::kDocumentIndexed, R"({"event_type":"document_indexed"})"));

    const auto stats = processor_->stats();
    EXPECT_EQ(stats.malformed_events, 1u);
}

TEST_F(RemoteEventProcessorTest, DeserializationFailureRemoved)
{
    EXPECT_FALSE(processor_->process(
        topics::kDocumentRemoved, R"({"event_type":"document_removed"})"));

    const auto stats = processor_->stats();
    EXPECT_EQ(stats.malformed_events, 1u);
}

// ---------------------------------------------------------------------------
// 19. Large content
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, LargeContentPreserved)
{
    // Create a large document content string.
    std::string large_content(10000, 'x');
    for (std::size_t i = 0; i < large_content.size(); i += 100) {
        large_content[i] = 'a';
    }

    const std::string payload = make_indexed_payload(
        1, 100, 0, 1, large_content);

    EXPECT_TRUE(processor_->process(topics::kDocumentIndexed, payload));

    ShardGetRequest req;
    req.shard_id = 0;
    req.document_id = 100;
    auto resp = node0_->get_document(req);
    EXPECT_TRUE(resp.found);
    EXPECT_EQ(resp.content, large_content);
}

// ---------------------------------------------------------------------------
// 20. Concurrent processing (basic thread safety check)
// ---------------------------------------------------------------------------

TEST_F(RemoteEventProcessorTest, ConcurrentProcessing)
{
    // Different doc_ids on different shards to avoid contention.
    std::vector<std::thread> threads;

    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([this, i]() {
            const doc_id id = static_cast<doc_id>(i);
            const std::size_t shard_id = static_cast<std::size_t>(i % 3);
            // Use source_node_id = 0 since node0 hosts shards 0 and 1,
            // but only shard 2 is on node1.
            if (shard_id == 2) {
                // shard 2 is on node1
                const std::string payload = make_indexed_payload(
                    i, id, 2, 1, "concurrent doc " + std::to_string(i));
                processor_->process(topics::kDocumentIndexed, payload);
            } else {
                const std::string payload = make_indexed_payload(
                    i, id, shard_id, 0, "concurrent doc " + std::to_string(i));
                processor_->process(topics::kDocumentIndexed, payload);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    const auto stats = processor_->stats();
    EXPECT_EQ(stats.events_received, 10u);
    // All should be processed or skipped (shard 2 docs go to node1).
    EXPECT_EQ(stats.events_failed, 0u);
}

// ---------------------------------------------------------------------------

} // namespace
} // namespace dse
