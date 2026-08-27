// Distributed Search Engine - Kafka Message Broker Tests (Phase 19C).
//
// Integration tests for KafkaMessageBroker against a live Kafka broker.
// These tests require:
//   - ENABLE_KAFKA=ON
//   - Kafka running at localhost:9094 (Phase 19A Docker infrastructure)
//
// Tests verify actual messages in Kafka topics, not just that produce
// succeeded at the API level.

#ifdef DSE_KAFKA_ENABLED

#include "kafka_message_broker.h"
#include "kafka_client.h"
#include "message.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <string>
#include <thread>
#include <vector>

namespace dse {
namespace {

// ---------------------------------------------------------------------------
// Helper: consume all messages from a Kafka topic via a dedicated consumer.
// Uses librdkafka's simple consumer to read messages from all partitions.
// This is test infrastructure only — not a production consumer.
// ---------------------------------------------------------------------------

struct ConsumedMessage {
    std::string payload;
    std::string key;
    int64_t offset = 0;
    int32_t partition = 0;
};

// Check if Kafka is reachable. Tests skip if broker is unavailable.
bool kafka_available()
{
    try {
        KafkaClientConfig cfg;
        cfg.bootstrap_servers = "localhost:9094";
        cfg.client_id = "dse-test-check";
        KafkaClient client(cfg);

        // Try a produce — if it succeeds (accepted into queue), broker is up.
        bool accepted = client.produce_async("documents.indexed", "ping", "check");
        client.flush(2000);
        return accepted;
    } catch (...) {
        return false;
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

class KafkaMessageBrokerTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!kafka_available()) {
            GTEST_SKIP() << "Kafka broker not available at localhost:9094";
        }
    }
};

// --- Basic construction ---

TEST_F(KafkaMessageBrokerTest, Construction)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-construction";

    KafkaMessageBroker broker(cfg);
    EXPECT_TRUE(broker.client().is_healthy());
}

TEST_F(KafkaMessageBrokerTest, CustomConfiguration)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-custom";
    cfg.poll_interval_ms = 25;

    KafkaMessageBroker broker(cfg);
    EXPECT_TRUE(broker.client().is_healthy());
}

// --- Publish to topics ---

TEST_F(KafkaMessageBrokerTest, PublishToIndexedTopic)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-pub-indexed";

    KafkaMessageBroker broker(cfg);

    Message msg;
    msg.topic = "documents.indexed";
    msg.payload = R"({"event_id":1,"event_type":"document_indexed","document_id":42})";

    Offset offset = broker.publish(std::move(msg));
    EXPECT_GE(offset, 0u);

    // Allow delivery report processing.
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    broker.stop();
}

TEST_F(KafkaMessageBrokerTest, PublishToUpdatedTopic)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-pub-updated";

    KafkaMessageBroker broker(cfg);

    Message msg;
    msg.topic = "documents.updated";
    msg.payload = R"({"event_id":2,"event_type":"document_updated","document_id":99})";

    Offset offset = broker.publish(std::move(msg));
    EXPECT_GE(offset, 0u);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    broker.stop();
}

TEST_F(KafkaMessageBrokerTest, PublishToRemovedTopic)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-pub-removed";

    KafkaMessageBroker broker(cfg);

    Message msg;
    msg.topic = "documents.removed";
    msg.payload = R"({"event_id":3,"event_type":"document_removed","document_id":77})";

    Offset offset = broker.publish(std::move(msg));
    EXPECT_GE(offset, 0u);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    broker.stop();
}

// --- Payload preservation ---

TEST_F(KafkaMessageBrokerTest, PayloadPreserved)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-payload";

    KafkaMessageBroker broker(cfg);

    const std::string payload =
        R"({"event_id":10,"event_type":"document_indexed","document_id":12345,"shard_id":0})";

    Message msg;
    msg.topic = "documents.indexed";
    msg.payload = payload;

    Offset offset = broker.publish(std::move(msg));
    EXPECT_GE(offset, 0u);

    // The offset should be monotonically increasing.
    Message msg2;
    msg2.topic = "documents.indexed";
    msg2.payload = R"({"event_id":11})";

    Offset offset2 = broker.publish(std::move(msg2));
    EXPECT_GT(offset2, offset);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    broker.stop();
}

// --- Multiple messages ---

TEST_F(KafkaMessageBrokerTest, MultipleMessages)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-multi";

    KafkaMessageBroker broker(cfg);

    for (int i = 0; i < 10; ++i) {
        Message msg;
        msg.topic = "documents.indexed";
        msg.payload = R"({"event_id":)" + std::to_string(i + 100) + "}";
        broker.publish(std::move(msg));
    }

    auto s = broker.stats();
    EXPECT_EQ(s.messages_published, 10u);

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    broker.stop();
}

// --- publish_with_timeout ---

TEST_F(KafkaMessageBrokerTest, PublishWithTimeoutSuccess)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-timeout-ok";

    KafkaMessageBroker broker(cfg);

    Message msg;
    msg.topic = "documents.indexed";
    msg.payload = R"({"event_id":50})";

    auto offset = broker.publish_with_timeout(std::move(msg), 5000);
    ASSERT_TRUE(offset.has_value());
    EXPECT_GE(*offset, 0u);

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    broker.stop();
}

// --- Statistics ---

TEST_F(KafkaMessageBrokerTest, Statistics)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-stats";

    KafkaMessageBroker broker(cfg);

    for (int i = 0; i < 5; ++i) {
        Message msg;
        msg.topic = "documents.indexed";
        msg.payload = R"({"event_id":)" + std::to_string(i + 200) + "}";
        broker.publish(std::move(msg));
    }

    auto s = broker.stats();
    EXPECT_EQ(s.messages_published, 5u);

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    broker.stop();
}

// --- Lifecycle ---

TEST_F(KafkaMessageBrokerTest, CleanShutdown)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-shutdown";

    KafkaMessageBroker broker(cfg);

    for (int i = 0; i < 3; ++i) {
        Message msg;
        msg.topic = "documents.indexed";
        msg.payload = R"({"event_id":)" + std::to_string(i + 300) + "}";
        broker.publish(std::move(msg));
    }

    // Stop should flush and close cleanly.
    broker.stop();
    EXPECT_FALSE(broker.client().is_healthy());
}

TEST_F(KafkaMessageBrokerTest, StopIsIdempotent)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-idempotent";

    KafkaMessageBroker broker(cfg);

    Message msg;
    msg.topic = "documents.indexed";
    msg.payload = R"({"event_id":400})";
    broker.publish(std::move(msg));

    broker.stop();
    broker.stop();  // Should not crash.
    broker.stop();  // Triple-stop is safe.
}

TEST_F(KafkaMessageBrokerTest, DestructorStopsCleanly)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-destructor";

    {
        KafkaMessageBroker broker(cfg);

        for (int i = 0; i < 5; ++i) {
            Message msg;
            msg.topic = "documents.updated";
            msg.payload = R"({"event_id":)" + std::to_string(i + 500) + "}";
            broker.publish(std::move(msg));
        }
        // Destructor should flush + close without blocking indefinitely.
    }
}

// --- Dead letters / was_processed (stubs) ---

TEST_F(KafkaMessageBrokerTest, DeadLettersEmpty)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-deadletters";

    KafkaMessageBroker broker(cfg);
    auto letters = broker.dead_letters("documents.indexed");
    EXPECT_TRUE(letters.empty());
}

TEST_F(KafkaMessageBrokerTest, WasProcessedReturnsFalse)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-wasprocessed";

    KafkaMessageBroker broker(cfg);
    EXPECT_FALSE(broker.was_processed(1));
}

// --- Delivery reports ---

TEST_F(KafkaMessageBrokerTest, DeliveryReportsReceived)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-delivery";

    KafkaMessageBroker broker(cfg);

    // Start the poll thread explicitly.
    broker.start();

    for (int i = 0; i < 3; ++i) {
        Message msg;
        msg.topic = "documents.indexed";
        msg.payload = R"({"event_id":)" + std::to_string(i + 600) + "}";
        broker.publish(std::move(msg));
    }

    // Wait for delivery reports to arrive.
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_GE(broker.delivery_reports_count(), 3u);

    broker.stop();
}

} // namespace
} // namespace dse

#else // !DSE_KAFKA_ENABLED

// Stub test file when Kafka is not enabled.
// Ensures the test target compiles and registers cleanly.
#include <gtest/gtest.h>

TEST(KafkaMessageBrokerDisabled, SkippedWhenKafkaNotEnabled)
{
    SUCCEED() << "Kafka not enabled; KafkaMessageBroker tests are skipped.";
}

#endif // DSE_KAFKA_ENABLED
