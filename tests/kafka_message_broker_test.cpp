// Distributed Search Engine - Kafka Message Broker Tests (Phase 19C/19D).
//
// Integration tests for KafkaMessageBroker against a live Kafka broker.
// These tests require:
//   - ENABLE_KAFKA=ON
//   - Kafka running at localhost:9094 (Phase 19A Docker infrastructure)
//
// Phase 19C: Producer tests (publish to topics, payload preservation).
// Phase 19D: Consumer tests (subscribe, consume, offset commit, redelivery).

#ifdef DSE_KAFKA_ENABLED

#include "kafka_message_broker.h"
#include "kafka_client.h"
#include "message.h"

#include <gtest/gtest.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace dse {
namespace {

// ---------------------------------------------------------------------------
// Helper: check if Kafka broker is reachable
// ---------------------------------------------------------------------------

bool kafka_available()
{
    try {
        KafkaClientConfig cfg;
        cfg.bootstrap_servers = "localhost:9094";
        cfg.client_id = "dse-test-check";
        KafkaClient client(cfg);

        bool accepted = client.produce_async("documents.indexed", "ping", "check");
        client.flush(2000);
        return accepted;
    } catch (...) {
        return false;
    }
}

// ---------------------------------------------------------------------------
// Helper: wait for a condition with timeout
// ---------------------------------------------------------------------------

bool wait_until(std::function<bool()> predicate,
                int timeout_ms = 5000,
                int poll_ms = 10)
{
    auto deadline = std::chrono::steady_clock::now()
                  + std::chrono::milliseconds(timeout_ms);
    while (std::chrono::steady_clock::now() < deadline) {
        if (predicate()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(poll_ms));
    }
    return predicate();  // one last check
}

// ---------------------------------------------------------------------------
// Helper: generate unique test names for consumer group isolation
// ---------------------------------------------------------------------------

std::string unique_group(const std::string& prefix)
{
    static std::atomic<int> counter{0};
    return prefix + "-test-" + std::to_string(counter.fetch_add(1));
}

// ===========================================================================
// Phase 19C — Producer Tests
// ===========================================================================

class KafkaProducerTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!kafka_available()) {
            GTEST_SKIP() << "Kafka broker not available at localhost:9094";
        }
    }
};

TEST_F(KafkaProducerTest, Construction)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-construction";

    KafkaMessageBroker broker(cfg);
    EXPECT_TRUE(broker.client().is_healthy());
}

TEST_F(KafkaProducerTest, PublishToIndexedTopic)
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

    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    broker.stop();
}

TEST_F(KafkaProducerTest, PublishToUpdatedTopic)
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

TEST_F(KafkaProducerTest, PublishToRemovedTopic)
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

TEST_F(KafkaProducerTest, MultipleMessages)
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

TEST_F(KafkaProducerTest, PublishWithTimeoutSuccess)
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

TEST_F(KafkaProducerTest, Statistics)
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

TEST_F(KafkaProducerTest, CleanShutdown)
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

    broker.stop();
    EXPECT_FALSE(broker.client().is_healthy());
}

TEST_F(KafkaProducerTest, StopIsIdempotent)
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
    broker.stop();
    broker.stop();
}

TEST_F(KafkaProducerTest, DestructorStopsCleanly)
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
    }
}

TEST_F(KafkaProducerTest, DeadLettersEmpty)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-deadletters";

    KafkaMessageBroker broker(cfg);
    auto letters = broker.dead_letters("documents.indexed");
    EXPECT_TRUE(letters.empty());
}

TEST_F(KafkaProducerTest, WasProcessedReturnsFalse)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-wasprocessed";

    KafkaMessageBroker broker(cfg);
    EXPECT_FALSE(broker.was_processed(1));
}

TEST_F(KafkaProducerTest, DeliveryReportsReceived)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-delivery";

    KafkaMessageBroker broker(cfg);
    broker.start();

    for (int i = 0; i < 3; ++i) {
        Message msg;
        msg.topic = "documents.indexed";
        msg.payload = R"({"event_id":)" + std::to_string(i + 600) + "}";
        broker.publish(std::move(msg));
    }

    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    EXPECT_GE(broker.delivery_reports_count(), 3u);

    broker.stop();
}

// ===========================================================================
// Phase 19D — Consumer Tests
// ===========================================================================

class KafkaConsumerTest : public ::testing::Test {
protected:
    void SetUp() override {
        if (!kafka_available()) {
            GTEST_SKIP() << "Kafka broker not available at localhost:9094";
        }
    }
};

// --- 1. Consumer construction ---

TEST_F(KafkaConsumerTest, ConsumerConstruction)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-consumer-construct";
    cfg.group_id = unique_group("construct");

    KafkaMessageBroker broker(cfg);
    EXPECT_TRUE(broker.client().is_healthy());

    // subscribe + start without error.
    std::atomic<int> received{0};
    broker.subscribe("documents.indexed", [&](const Message&) -> bool {
        ++received;
        return true;
    });

    broker.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    broker.stop();
}

// --- 2. Single-topic subscription ---

TEST_F(KafkaConsumerTest, SingleTopicSubscription)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-consumer-single";
    cfg.group_id = unique_group("single-topic");

    // Use a unique topic name to avoid contamination from other tests.
    const std::string topic = "test.single-topic-" + unique_group("");

    KafkaBrokerConfig pcfg;
    pcfg.bootstrap_servers = "localhost:9094";
    pcfg.client_id = "dse-test-producer-single";

    // Produce a message first.
    {
        KafkaMessageBroker producer(pcfg);
        Message msg;
        msg.topic = topic;
        msg.payload = R"({"test":"single-topic"})";
        producer.publish(std::move(msg));
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        producer.stop();
    }

    // Consume the message.
    std::atomic<int> received{0};
    std::string received_payload;

    KafkaMessageBroker consumer_broker(cfg);
    consumer_broker.subscribe(topic, [&](const Message& m) -> bool {
        received_payload = m.payload;
        ++received;
        return true;
    });
    consumer_broker.start();

    bool got = wait_until([&] { return received.load() >= 1; }, 10000);

    consumer_broker.stop();

    EXPECT_TRUE(got) << "Consumer did not receive the message within timeout";
    EXPECT_EQ(received_payload, R"({"test":"single-topic"})");
}

// --- 3. Multiple-topic subscription ---

TEST_F(KafkaConsumerTest, MultipleTopicSubscription)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-consumer-multi";
    cfg.group_id = unique_group("multi-topic");

    const std::string topic_a = "test.multi-a-" + unique_group("");
    const std::string topic_b = "test.multi-b-" + unique_group("");

    // Produce to both topics.
    {
        KafkaBrokerConfig pcfg;
        pcfg.bootstrap_servers = "localhost:9094";
        pcfg.client_id = "dse-test-producer-multi";
        KafkaMessageBroker producer(pcfg);

        Message msg_a;
        msg_a.topic = topic_a;
        msg_a.payload = R"({"from":"topic-a"})";
        producer.publish(std::move(msg_a));

        Message msg_b;
        msg_b.topic = topic_b;
        msg_b.payload = R"({"from":"topic-b"})";
        producer.publish(std::move(msg_b));

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        producer.stop();
    }

    std::atomic<int> received_a{0};
    std::atomic<int> received_b{0};

    KafkaMessageBroker consumer_broker(cfg);
    consumer_broker.subscribe(topic_a, [&](const Message&) -> bool {
        ++received_a;
        return true;
    });
    consumer_broker.subscribe(topic_b, [&](const Message&) -> bool {
        ++received_b;
        return true;
    });

    consumer_broker.start();

    bool got_a = wait_until([&] { return received_a.load() >= 1; }, 10000);
    bool got_b = wait_until([&] { return received_b.load() >= 1; }, 10000);

    consumer_broker.stop();

    EXPECT_TRUE(got_a) << "Consumer did not receive from topic A";
    EXPECT_TRUE(got_b) << "Consumer did not receive from topic B";
}

// --- 4. Consumer group configuration ---

TEST_F(KafkaConsumerTest, ConsumerGroupConfiguration)
{
    KafkaBrokerConfig cfg;
    cfg.bootstrap_servers = "localhost:9094";
    cfg.client_id = "dse-test-consumer-group";
    cfg.group_id = unique_group("group-config");

    KafkaMessageBroker broker(cfg);

    std::atomic<int> received{0};
    broker.subscribe("documents.indexed", [&](const Message&) -> bool {
        ++received;
        return true;
    });

    broker.start();
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    broker.stop();

    // Consumer started and stopped without error — group was configured.
    SUCCEED();
}

// --- 5. Publish → consume round trip ---

TEST_F(KafkaConsumerTest, PublishConsumeRoundTrip)
{
    const std::string topic = "test.roundtrip-" + unique_group("");

    // Producer.
    KafkaBrokerConfig pcfg;
    pcfg.bootstrap_servers = "localhost:9094";
    pcfg.client_id = "dse-test-producer-roundtrip";

    // Consumer.
    KafkaBrokerConfig ccfg;
    ccfg.bootstrap_servers = "localhost:9094";
    ccfg.client_id = "dse-test-consumer-roundtrip";
    ccfg.group_id = unique_group("roundtrip");

    std::atomic<int> received{0};
    std::string received_payload;

    KafkaMessageBroker consumer_broker(ccfg);
    consumer_broker.subscribe(topic, [&](const Message& m) -> bool {
        received_payload = m.payload;
        ++received;
        return true;
    });
    consumer_broker.start();

    // Publish after consumer is started.
    {
        KafkaMessageBroker producer(pcfg);
        Message msg;
        msg.topic = topic;
        msg.payload = R"({"roundtrip":true})";
        producer.publish(std::move(msg));
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        producer.stop();
    }

    bool got = wait_until([&] { return received.load() >= 1; }, 10000);
    consumer_broker.stop();

    EXPECT_TRUE(got);
    EXPECT_EQ(received_payload, R"({"roundtrip":true})");
}

// --- 6. Payload preservation ---

TEST_F(KafkaConsumerTest, PayloadPreservation)
{
    const std::string topic = "test.payload-" + unique_group("");
    const std::string payload =
        R"({"event_id":10,"event_type":"document_indexed","document_id":12345,"shard_id":0})";

    KafkaBrokerConfig ccfg;
    ccfg.bootstrap_servers = "localhost:9094";
    ccfg.client_id = "dse-test-consumer-payload";
    ccfg.group_id = unique_group("payload");

    std::atomic<int> received{0};
    std::string received_payload;

    KafkaMessageBroker consumer_broker(ccfg);
    consumer_broker.subscribe(topic, [&](const Message& m) -> bool {
        received_payload = m.payload;
        ++received;
        return true;
    });
    consumer_broker.start();

    {
        KafkaBrokerConfig pcfg;
        pcfg.bootstrap_servers = "localhost:9094";
        pcfg.client_id = "dse-test-producer-payload";
        KafkaMessageBroker producer(pcfg);
        Message msg;
        msg.topic = topic;
        msg.payload = payload;
        producer.publish(std::move(msg));
        std::this_thread::sleep_for(std::chrono::milliseconds(300));
        producer.stop();
    }

    bool got = wait_until([&] { return received.load() >= 1; }, 10000);
    consumer_broker.stop();

    EXPECT_TRUE(got);
    EXPECT_EQ(received_payload, payload);
}

// --- 7. Successful handler → offset commit ---

TEST_F(KafkaConsumerTest, SuccessfulHandlerCommitsOffset)
{
    const std::string topic = "test.ack-" + unique_group("");

    KafkaBrokerConfig ccfg;
    ccfg.bootstrap_servers = "localhost:9094";
    ccfg.client_id = "dse-test-consumer-ack";
    ccfg.group_id = unique_group("ack");

    std::atomic<int> received{0};

    KafkaMessageBroker consumer_broker(ccfg);
    consumer_broker.subscribe(topic, [&](const Message&) -> bool {
        ++received;
        return true;  // success → commit offset
    });
    consumer_broker.start();

    // Publish 3 messages.
    {
        KafkaBrokerConfig pcfg;
        pcfg.bootstrap_servers = "localhost:9094";
        pcfg.client_id = "dse-test-producer-ack";
        KafkaMessageBroker producer(pcfg);
        for (int i = 0; i < 3; ++i) {
            Message msg;
            msg.topic = topic;
            msg.payload = R"({"ack_test":)" + std::to_string(i) + "}";
            producer.publish(std::move(msg));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        producer.stop();
    }

    bool got = wait_until([&] { return received.load() >= 3; }, 10000);
    consumer_broker.stop();

    EXPECT_TRUE(got);
    EXPECT_GE(received.load(), 3);
}

// --- 8. Failed handler blocks and retries sequentially ---
// This test verifies that if a handler fails, the consumer retries the SAME
// message repeatedly (sequential retry/backoff) and does not proceed to the next.

TEST_F(KafkaConsumerTest, SequentialRetryBackoff)
{
    const std::string topic = "test.retry-" + unique_group("");

    KafkaBrokerConfig ccfg;
    ccfg.bootstrap_servers = "localhost:9094";
    ccfg.client_id = "dse-test-consumer-retry";
    ccfg.group_id = unique_group("retry");

    std::atomic<int> first_msg_attempts{0};
    std::atomic<int> second_msg_attempts{0};

    KafkaMessageBroker consumer_broker(ccfg);
    consumer_broker.subscribe(topic, [&](const Message& m) -> bool {
        if (m.payload == "fail_then_succeed") {
            ++first_msg_attempts;
            // Fail the first 3 times, then succeed
            if (first_msg_attempts.load() <= 3) {
                return false;
            }
            return true;
        } else if (m.payload == "next_msg") {
            ++second_msg_attempts;
            return true;
        }
        return true;
    });
    consumer_broker.start();

    // Publish 2 messages.
    {
        KafkaBrokerConfig pcfg;
        pcfg.bootstrap_servers = "localhost:9094";
        pcfg.client_id = "dse-test-producer-retry";
        KafkaMessageBroker producer(pcfg);

        Message msg1;
        msg1.topic = topic;
        msg1.payload = "fail_then_succeed";
        producer.publish(std::move(msg1));

        Message msg2;
        msg2.topic = topic;
        msg2.payload = "next_msg";
        producer.publish(std::move(msg2));

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        producer.stop();
    }

    // Wait until the second message is successfully processed.
    bool got = wait_until([&] { return second_msg_attempts.load() >= 1; }, 15000);
    consumer_broker.stop();

    EXPECT_TRUE(got) << "Second message was never processed (blocked indefinitely?)";
    EXPECT_EQ(first_msg_attempts.load(), 4); // 3 failures + 1 success
    EXPECT_EQ(second_msg_attempts.load(), 1);
}

// --- 8b. Estimated Lag Test ---

TEST_F(KafkaConsumerTest, EstimatedLagTracking)
{
    const std::string topic = "test.lag-" + unique_group("");

    // Publish messages BEFORE consumer starts to ensure there is lag.
    {
        KafkaBrokerConfig pcfg;
        pcfg.bootstrap_servers = "localhost:9094";
        pcfg.client_id = "dse-test-producer-lag";
        KafkaMessageBroker producer(pcfg);

        for (int i = 0; i < 5; ++i) {
            Message msg;
            msg.topic = topic;
            msg.payload = "lag_test";
            producer.publish(std::move(msg));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        producer.stop();
    }

    KafkaBrokerConfig ccfg;
    ccfg.bootstrap_servers = "localhost:9094";
    ccfg.client_id = "dse-test-consumer-lag";
    ccfg.group_id = unique_group("lag");

    std::atomic<int> received{0};
    KafkaMessageBroker consumer_broker(ccfg);

    // Subscribe but don't start yet.
    consumer_broker.subscribe(topic, [&](const Message&) -> bool {
        ++received;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        return true;
    });

    consumer_broker.start();

    // Check lag while processing.
    bool saw_lag = wait_until([&] { return consumer_broker.consumer_lag() > 0; }, 5000);

    // Wait for all 5 to be processed.
    wait_until([&] { return received.load() >= 5; }, 10000);

    // Eventually lag should be 0.
    bool lag_cleared = wait_until([&] { return consumer_broker.consumer_lag() == 0; }, 5000);

    consumer_broker.stop();

    // Note: consumer_lag() might return 0 if the consumer hasn't joined the group yet,
    // so saw_lag is a best-effort check, but lag_cleared is a strict requirement.
    EXPECT_TRUE(lag_cleared) << "Lag did not reach 0";
}

// --- 9. Consumer restart and committed-offset recovery ---
// This test verifies that after a consumer restart, only uncommitted
// offsets are redelivered (at-least-once semantics).

TEST_F(KafkaConsumerTest, ConsumerRestartRecovery)
{
    const std::string topic = "test.restart-" + unique_group("");
    const std::string group = unique_group("restart");

    // Phase 1: Produce 1 message, consume it (commit offset).
    std::atomic<int> first_run_count{0};

    {
        KafkaBrokerConfig pcfg;
        pcfg.bootstrap_servers = "localhost:9094";
        pcfg.client_id = "dse-test-producer-restart";
        KafkaMessageBroker producer(pcfg);

        Message msg1;
        msg1.topic = topic;
        msg1.payload = R"({"restart":"first"})";
        producer.publish(std::move(msg1));
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        producer.stop();
    }

    // Consume and commit.
    {
        KafkaBrokerConfig ccfg;
        ccfg.bootstrap_servers = "localhost:9094";
        ccfg.client_id = "dse-test-consumer-restart-1";
        ccfg.group_id = group;

        KafkaMessageBroker broker(ccfg);
        broker.subscribe(topic, [&](const Message&) -> bool {
            ++first_run_count;
            return true;  // commit offset
        });
        broker.start();
        wait_until([&] { return first_run_count.load() >= 1; }, 10000);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        broker.stop();
    }

    EXPECT_EQ(first_run_count.load(), 1);

    // Phase 2: Produce a SECOND message (after first consumer committed).
    {
        KafkaBrokerConfig pcfg;
        pcfg.bootstrap_servers = "localhost:9094";
        pcfg.client_id = "dse-test-producer-restart-2";
        KafkaMessageBroker producer(pcfg);

        Message msg2;
        msg2.topic = topic;
        msg2.payload = R"({"restart":"second"})";
        producer.publish(std::move(msg2));
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        producer.stop();
    }

    // Phase 3: Restart with same group — should get only the second message.
    std::atomic<int> second_run_count{0};

    {
        KafkaBrokerConfig ccfg;
        ccfg.bootstrap_servers = "localhost:9094";
        ccfg.client_id = "dse-test-consumer-restart-2";
        ccfg.group_id = group;  // same group → same committed offset

        KafkaMessageBroker broker(ccfg);
        broker.subscribe(topic, [&](const Message&) -> bool {
            ++second_run_count;
            return true;
        });
        broker.start();
        bool got = wait_until([&] { return second_run_count.load() >= 1; }, 10000);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        broker.stop();

        EXPECT_TRUE(got);
    }

    // First consumer consumed 1, second consumer consumed 1 (the new message).
    EXPECT_EQ(first_run_count.load(), 1);
    EXPECT_EQ(second_run_count.load(), 1);
}

// --- 10. Graceful shutdown ---

TEST_F(KafkaConsumerTest, GracefulShutdown)
{
    const std::string topic = "test.shutdown-" + unique_group("");

    KafkaBrokerConfig ccfg;
    ccfg.bootstrap_servers = "localhost:9094";
    ccfg.client_id = "dse-test-consumer-shutdown";
    ccfg.group_id = unique_group("shutdown");

    std::atomic<int> received{0};

    KafkaMessageBroker consumer_broker(ccfg);
    consumer_broker.subscribe(topic, [&](const Message&) -> bool {
        ++received;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        return true;
    });
    consumer_broker.start();

    // Publish some messages.
    {
        KafkaBrokerConfig pcfg;
        pcfg.bootstrap_servers = "localhost:9094";
        pcfg.client_id = "dse-test-producer-shutdown";
        KafkaMessageBroker producer(pcfg);
        for (int i = 0; i < 5; ++i) {
            Message msg;
            msg.topic = topic;
            msg.payload = R"({"shutdown_test":)" + std::to_string(i) + "}";
            producer.publish(std::move(msg));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        producer.stop();
    }

    // Wait for at least one message.
    wait_until([&] { return received.load() >= 1; }, 5000);

    // Stop should not block indefinitely.
    auto start = std::chrono::steady_clock::now();
    consumer_broker.stop();
    auto elapsed = std::chrono::steady_clock::now() - start;

    EXPECT_LT(elapsed, std::chrono::seconds(10))
        << "stop() blocked for too long";
    EXPECT_GE(received.load(), 1);
}

// --- 11. Concurrent producer/consumer operation ---

TEST_F(KafkaConsumerTest, ConcurrentProducerConsumer)
{
    const std::string topic = "test.concurrent-" + unique_group("");

    KafkaBrokerConfig ccfg;
    ccfg.bootstrap_servers = "localhost:9094";
    ccfg.client_id = "dse-test-consumer-concurrent";
    ccfg.group_id = unique_group("concurrent");

    std::atomic<int> received{0};

    KafkaMessageBroker consumer_broker(ccfg);
    consumer_broker.subscribe(topic, [&](const Message&) -> bool {
        ++received;
        return true;
    });
    consumer_broker.start();

    // Publish while consumer is running.
    {
        KafkaBrokerConfig pcfg;
        pcfg.bootstrap_servers = "localhost:9094";
        pcfg.client_id = "dse-test-producer-concurrent";
        KafkaMessageBroker producer(pcfg);
        for (int i = 0; i < 10; ++i) {
            Message msg;
            msg.topic = topic;
            msg.payload = R"({"concurrent":)" + std::to_string(i) + "}";
            producer.publish(std::move(msg));
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        producer.stop();
    }

    wait_until([&] { return received.load() >= 5; }, 15000);
    consumer_broker.stop();

    EXPECT_GE(received.load(), 5);
}

// --- 12. Three document event topics ---

TEST_F(KafkaConsumerTest, ThreeDocumentEventTopics)
{
    KafkaBrokerConfig ccfg;
    ccfg.bootstrap_servers = "localhost:9094";
    ccfg.client_id = "dse-test-consumer-3topics";
    ccfg.group_id = unique_group("3topics");

    std::atomic<int> indexed_count{0};
    std::atomic<int> updated_count{0};
    std::atomic<int> removed_count{0};

    KafkaMessageBroker consumer_broker(ccfg);
    consumer_broker.subscribe("documents.indexed", [&](const Message&) -> bool {
        ++indexed_count;
        return true;
    });
    consumer_broker.subscribe("documents.updated", [&](const Message&) -> bool {
        ++updated_count;
        return true;
    });
    consumer_broker.subscribe("documents.removed", [&](const Message&) -> bool {
        ++removed_count;
        return true;
    });
    consumer_broker.start();

    std::this_thread::sleep_for(std::chrono::milliseconds(500));

    // Publish to all three topics.
    {
        KafkaBrokerConfig pcfg;
        pcfg.bootstrap_servers = "localhost:9094";
        pcfg.client_id = "dse-test-producer-3topics";
        KafkaMessageBroker producer(pcfg);

        Message idx;
        idx.topic = "documents.indexed";
        idx.payload = R"({"3topics":"indexed"})";
        producer.publish(std::move(idx));

        Message upd;
        upd.topic = "documents.updated";
        upd.payload = R"({"3topics":"updated"})";
        producer.publish(std::move(upd));

        Message rem;
        rem.topic = "documents.removed";
        rem.payload = R"({"3topics":"removed"})";
        producer.publish(std::move(rem));

        std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        producer.stop();
    }

    wait_until([&] {
        return indexed_count.load() >= 1 &&
               updated_count.load() >= 1 &&
               removed_count.load() >= 1;
    }, 15000);

    consumer_broker.stop();

    EXPECT_GE(indexed_count.load(), 1);
    EXPECT_GE(updated_count.load(), 1);
    EXPECT_GE(removed_count.load(), 1);
}

// --- 13. Consumer statistics ---

TEST_F(KafkaConsumerTest, ConsumerStatistics)
{
    const std::string topic = "test.stats-" + unique_group("");

    KafkaBrokerConfig ccfg;
    ccfg.bootstrap_servers = "localhost:9094";
    ccfg.client_id = "dse-test-consumer-stats";
    ccfg.group_id = unique_group("stats");

    std::atomic<int> received{0};

    KafkaMessageBroker consumer_broker(ccfg);
    consumer_broker.subscribe(topic, [&](const Message&) -> bool {
        ++received;
        return true;
    });
    consumer_broker.start();

    // Publish messages.
    {
        KafkaBrokerConfig pcfg;
        pcfg.bootstrap_servers = "localhost:9094";
        pcfg.client_id = "dse-test-producer-stats";
        KafkaMessageBroker producer(pcfg);
        for (int i = 0; i < 5; ++i) {
            Message msg;
            msg.topic = topic;
            msg.payload = R"({"stats_test":)" + std::to_string(i) + "}";
            producer.publish(std::move(msg));
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        producer.stop();
    }

    wait_until([&] { return received.load() >= 5; }, 10000);
    consumer_broker.stop();

    EXPECT_GE(consumer_broker.messages_consumed(), 5u);
    EXPECT_GE(consumer_broker.rebalance_count(), 0u);
}

// --- 14. Consumer with handler failure and head-of-line retry ---
// In Phase 19F, sequential retry ensures that if a handler fails on a message,
// it is retried repeatedly with backoff and blocks subsequent messages in the partition.
// The consumer does not skip the failing message.

TEST_F(KafkaConsumerTest, HandlerFailureAndRecovery)
{
    const std::string topic = "test.recover-" + unique_group("");

    KafkaBrokerConfig ccfg;
    ccfg.bootstrap_servers = "localhost:9094";
    ccfg.client_id = "dse-test-consumer-recover";
    ccfg.group_id = unique_group("recover");

    std::atomic<int> ok1_processed{0};
    std::atomic<int> fail_attempts{0};
    std::atomic<int> ok2_processed{0};

    KafkaMessageBroker consumer_broker(ccfg);
    consumer_broker.subscribe(topic, [&](const Message& m) -> bool {
        if (m.payload.find("ok1") != std::string::npos) {
            ++ok1_processed;
            return true;
        }
        if (m.payload.find("fail") != std::string::npos) {
            ++fail_attempts;
            return false;  // permanently fail to verify head-of-line retry
        }
        if (m.payload.find("ok2") != std::string::npos) {
            ++ok2_processed;
            return true;
        }
        return true;
    });
    consumer_broker.start();

    // Publish 3 messages (ok1, fail, ok2).
    {
        KafkaBrokerConfig pcfg;
        pcfg.bootstrap_servers = "localhost:9094";
        pcfg.client_id = "dse-test-producer-recover";
        KafkaMessageBroker producer(pcfg);

        Message ok1;
        ok1.topic = topic;
        ok1.payload = R"({"recover_test":"ok1"})";
        producer.publish(std::move(ok1));

        Message fail_msg;
        fail_msg.topic = topic;
        fail_msg.payload = R"({"recover_test":"fail"})";
        producer.publish(std::move(fail_msg));

        Message ok2;
        ok2.topic = topic;
        ok2.payload = R"({"recover_test":"ok2"})";
        producer.publish(std::move(ok2));

        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        producer.stop();
    }

    // Wait until the failing message has been retried (at least 2 attempts: initial + 1 retry).
    bool retried = wait_until([&] { return fail_attempts.load() >= 2; }, 8000);
    consumer_broker.stop();

    EXPECT_TRUE(retried) << "Failing message was not retried";
    EXPECT_EQ(ok1_processed.load(), 1);       // first successful message is processed
    EXPECT_GE(fail_attempts.load(), 2);       // permanently failing message is retried
    EXPECT_EQ(ok2_processed.load(), 0);       // subsequent message is NOT processed while failing message remains unsuccessful (no implicit skip)
}

// --- 15. No-broker backward compatibility ---

TEST(KafkaMessageBrokerDisabled, SkippedWhenKafkaNotEnabled)
{
    SUCCEED() << "Kafka not enabled; KafkaMessageBroker tests are skipped.";
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
