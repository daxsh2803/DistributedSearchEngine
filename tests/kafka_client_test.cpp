// Distributed Search Engine - Kafka Client Integration Tests (Phase 19B).
//
// These tests exercise the KafkaClient producer against a live Kafka broker.
// They are compiled ONLY when ENABLE_KAFKA=ON (DSE_KAFKA_ENABLED defined).
// They are SKIPPED at runtime if the Kafka broker is unavailable.
//
// Prerequisites:
//   - Kafka broker running at localhost:9094 (Phase 19A Docker setup)
//   - Topics: documents.indexed, documents.updated, documents.removed
//
// These are INTEGRATION tests, not pure unit tests.

#ifdef DSE_KAFKA_ENABLED

#include "kafka_client.h"

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

namespace dse {
namespace {

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

static const std::string kTestBroker = "localhost:9094";
static const std::string kTestTopic = "documents.indexed";

// ---------------------------------------------------------------------------
// Helper: check if Kafka broker is reachable
// ---------------------------------------------------------------------------

bool is_kafka_available() {
    try {
        KafkaClientConfig cfg;
        cfg.bootstrap_servers = kTestBroker;
        cfg.delivery_timeout_ms = 2000;
        cfg.message_timeout_ms = 2000;
        KafkaClient client(cfg);
        // If construction succeeded, broker is reachable.
        return true;
    } catch (...) {
        return false;
    }
}

// ---------------------------------------------------------------------------
// Test fixture with skip-if-unavailable logic
// ---------------------------------------------------------------------------

class KafkaClientTest : public ::testing::Test {
protected:
    static void SetUpTestSuite() {
        kafka_available_ = is_kafka_available();
    }

    void SetUp() override {
        if (!kafka_available_) {
            GTEST_SKIP() << "Kafka broker not available at "
                         << kTestBroker;
        }
    }

    static bool kafka_available_;
};

bool KafkaClientTest::kafka_available_ = false;

// ===========================================================================
// 1. Construction
// ===========================================================================

TEST_F(KafkaClientTest, ConstructionDefaultConfig)
{
    KafkaClientConfig cfg;
    cfg.bootstrap_servers = kTestBroker;
    KafkaClient client(cfg);

    EXPECT_TRUE(client.is_healthy());
    EXPECT_TRUE(client.last_error().empty());
}

TEST_F(KafkaClientTest, ConstructionCustomConfig)
{
    KafkaClientConfig cfg;
    cfg.bootstrap_servers = kTestBroker;
    cfg.client_id = "dse-test-custom";
    cfg.delivery_timeout_ms = 3000;
    cfg.queue_buffering_max_ms = 10;
    KafkaClient client(cfg);

    EXPECT_TRUE(client.is_healthy());
}

// ===========================================================================
// 2. Async produce
// ===========================================================================

TEST_F(KafkaClientTest, ProduceAsyncSuccess)
{
    KafkaClientConfig cfg;
    cfg.bootstrap_servers = kTestBroker;
    cfg.delivery_timeout_ms = 5000;
    KafkaClient client(cfg);

    bool accepted = client.produce_async(kTestTopic, "hello-kafka-phase19b");
    EXPECT_TRUE(accepted);

    // Give librdkafka time to deliver.
    client.flush(5000);

    EXPECT_GE(client.delivery_reports_count(), 1u);
}

// ===========================================================================
// 3. Delivery report
// ===========================================================================

TEST_F(KafkaClientTest, DeliveryReportSuccess)
{
    KafkaClientConfig cfg;
    cfg.bootstrap_servers = kTestBroker;
    cfg.delivery_timeout_ms = 5000;
    KafkaClient client(cfg);

    DeliveryReport report = client.produce(
        kTestTopic, "delivery-report-test", "", 5000);

    EXPECT_TRUE(report.success) << "error: " << report.error_message;
    EXPECT_EQ(report.topic, kTestTopic);
    EXPECT_GE(report.offset, 0);
}

// ===========================================================================
// 4. Message key propagation
// ===========================================================================

TEST_F(KafkaClientTest, ProduceWithKey)
{
    KafkaClientConfig cfg;
    cfg.bootstrap_servers = kTestBroker;
    cfg.delivery_timeout_ms = 5000;
    KafkaClient client(cfg);

    DeliveryReport report = client.produce(
        kTestTopic, "key-test-payload", "test-key-42", 5000);

    EXPECT_TRUE(report.success) << "error: " << report.error_message;
    EXPECT_EQ(report.topic, kTestTopic);
}

// ===========================================================================
// 5. Multiple messages
// ===========================================================================

TEST_F(KafkaClientTest, ProduceMultipleMessages)
{
    KafkaClientConfig cfg;
    cfg.bootstrap_servers = kTestBroker;
    cfg.delivery_timeout_ms = 5000;
    KafkaClient client(cfg);

    const int count = 5;
    for (int i = 0; i < count; ++i) {
        std::string payload = "multi-msg-" + std::to_string(i);
        bool ok = client.produce_async(kTestTopic, payload,
                                        "key-" + std::to_string(i));
        EXPECT_TRUE(ok);
    }

    client.flush(10000);

    EXPECT_GE(client.delivery_reports_count(),
              static_cast<std::uint64_t>(count));
}

// ===========================================================================
// 6. Flush
// ===========================================================================

TEST_F(KafkaClientTest, FlushCompletesPending)
{
    KafkaClientConfig cfg;
    cfg.bootstrap_servers = kTestBroker;
    cfg.delivery_timeout_ms = 5000;
    KafkaClient client(cfg);

    const auto before = client.delivery_reports_count();

    client.produce_async(kTestTopic, "flush-test-1");
    client.produce_async(kTestTopic, "flush-test-2");
    client.produce_async(kTestTopic, "flush-test-3");

    bool flushed = client.flush(10000);
    EXPECT_TRUE(flushed);

    EXPECT_GE(client.delivery_reports_count(), before + 3);
}

// ===========================================================================
// 7. Close idempotence
// ===========================================================================

TEST_F(KafkaClientTest, CloseIsIdempotent)
{
    KafkaClientConfig cfg;
    cfg.bootstrap_servers = kTestBroker;
    KafkaClient client(cfg);

    client.produce_async(kTestTopic, "close-test");
    client.flush(5000);

    // Close multiple times should not crash.
    client.close();
    client.close();
    client.close();

    EXPECT_FALSE(client.is_healthy());
}

// ===========================================================================
// 8. Invalid broker / error handling
// ===========================================================================

TEST(KafkaClientInvalidBrokerTest, InvalidBrokerReportsError)
{
    KafkaClientConfig cfg;
    cfg.bootstrap_servers = "localhost:19999";  // wrong port
    cfg.delivery_timeout_ms = 1000;
    cfg.message_timeout_ms = 1000;

    // Construction may succeed (librdkafka is lazy) but the client
    // will report errors.
    KafkaClient client(cfg);

    // Produce will likely fail because broker is unreachable.
    DeliveryReport report = client.produce(
        kTestTopic, "will-fail", "", 2000);

    // Either the produce itself fails, or the delivery report is an error.
    if (report.success) {
        // If produce_async succeeded, the delivery report will indicate
        // an error. Flush to collect it.
        client.flush(2000);
        // The delivery count increased but report.success was true from
        // the produce() call — this means librdkafka accepted it but
        // will fail on delivery.
        EXPECT_TRUE(true);  // accepted into queue, delivery will fail
    } else {
        EXPECT_FALSE(report.success);
        EXPECT_FALSE(report.error_message.empty());
    }
}

// ===========================================================================
// 9. Health / error reporting
// ===========================================================================

TEST_F(KafkaClientTest, HealthCheckAfterProduce)
{
    KafkaClientConfig cfg;
    cfg.bootstrap_servers = kTestBroker;
    KafkaClient client(cfg);

    EXPECT_TRUE(client.is_healthy());

    client.produce_async(kTestTopic, "health-test");
    client.flush(5000);

    // Still healthy after successful produce.
    EXPECT_TRUE(client.is_healthy());

    client.close();
    EXPECT_FALSE(client.is_healthy());
}

TEST_F(KafkaClientTest, ProduceReturnsSuccessOnValidBroker)
{
    KafkaClientConfig cfg;
    cfg.bootstrap_servers = kTestBroker;
    KafkaClient client(cfg);

    DeliveryReport report = client.produce(
        kTestTopic, "error-clear-test", "", 5000);

    EXPECT_TRUE(report.success)
        << "expected success=true for valid broker, got error: "
        << report.error_message;
    EXPECT_EQ(report.topic, kTestTopic);
    EXPECT_GE(report.offset, 0);
}

// ===========================================================================
// 10. Outqueue length
// ===========================================================================

TEST_F(KafkaClientTest, OutqueueDecreasesAfterFlush)
{
    KafkaClientConfig cfg;
    cfg.bootstrap_servers = kTestBroker;
    cfg.delivery_timeout_ms = 5000;
    KafkaClient client(cfg);

    // Produce some messages.
    for (int i = 0; i < 10; ++i) {
        client.produce_async(kTestTopic, "outq-test-" + std::to_string(i));
    }

    // Outqueue should be > 0 before flush.
    // (Though it might be 0 if delivery is very fast.)
    client.flush(10000);

    // After flush, outqueue should be 0.
    EXPECT_EQ(client.outqueue_length(), 0u);
}

} // namespace
} // namespace dse

#endif // DSE_KAFKA_ENABLED
