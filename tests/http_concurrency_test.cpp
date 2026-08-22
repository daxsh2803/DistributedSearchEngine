// Distributed Search Engine - HTTP Concurrency Tests (Phase 8A-3).
//
// Proves that the HTTP server handles multiple requests concurrently.
// cpp-httplib's built-in thread pool (default: max(8, hardware_concurrency-1)
// threads) dispatches requests to worker threads. The underlying DocumentStore
// and InvertedIndex are already thread-safe (Phase 8A-1, 8A-2), so concurrent
// requests should produce correct, consistent results.
//
// Tests cover:
//   1. Multiple simultaneous GET /search requests (different terms)
//   2. Concurrent searches for the same term
//   3. Concurrent GET /search and POST /documents (single request each)
//   4. Multiple concurrent POST /documents using unique IDs
//   5. Search after concurrent ingestion sees all documents
//   6. Clean shutdown after concurrent requests
//   7. Mixed reader/writer workload
//   8. Concurrent AND and OR queries
//
// NOTE: Concurrent duplicate ID ingestion is deferred to Phase 8B
// because IngestionService has a TOCTOU race under concurrent duplicate
// attempts (see comment in test 5 below).

#include <gtest/gtest.h>

#include "http_server.h"
#include "local_node.h"
#include "node_client.h"
#include "node_config.h"
#include "shard.h"
#include "shard_coordinator.h"
#include "shard_router.h"
#include "tokenizer.h"

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace {

using dse::doc_id;
using dse::LocalNode;
using dse::NodeClient;
using dse::Shard;
using dse::ShardCoordinator;
using dse::ShardPlacement;
using dse::ShardRouter;

// ---------------------------------------------------------------------------
// Test fixture: single-shard coordinator for backward-compatible concurrency tests
// ---------------------------------------------------------------------------

class HttpConcurrencyTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto router = std::make_unique<ShardRouter>(1);
        std::vector<std::size_t> placement = {0};
        auto shard_placement = std::make_unique<ShardPlacement>(1, 1, placement);
        auto node = std::make_unique<LocalNode>(0);
        node->add_shard(0, std::make_unique<Shard>());
        std::vector<std::unique_ptr<NodeClient>> nodes;
        nodes.push_back(std::move(node));
        coordinator_ = std::make_unique<ShardCoordinator>(
            std::move(router), std::move(shard_placement), std::move(nodes));

        // Seed with documents that have distinct terms for targeted searches.
        for (doc_id id = 1; id <= 20; ++id) {
            coordinator_->ingest({id,
                "document " + std::to_string(id) + " content term" + std::to_string(id)});
        }
        server_ = std::make_unique<dse::HttpServer>(*coordinator_);
    }

    void start_server() {
        server_thread_ = std::thread([this]() {
            server_->listen(0);
        });
        server_->wait_until_ready();
    }

    void TearDown() override {
        server_->stop();
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
    }

    std::pair<int, std::string> get(const std::string& path) {
        httplib::Client client("localhost", server_->port());
        client.set_connection_timeout(5);
        client.set_read_timeout(5);
        auto res = client.Get(path);
        if (!res) return {0, ""};
        return {res->status, res->body};
    }

    std::pair<int, std::string> post(const std::string& path,
                                     const std::string& json_body) {
        httplib::Client client("localhost", server_->port());
        client.set_connection_timeout(5);
        client.set_read_timeout(5);
        auto res = client.Post(path.c_str(),
                               json_body.c_str(),
                               "application/json");
        if (!res) return {0, ""};
        return {res->status, res->body};
    }

    std::unique_ptr<ShardCoordinator> coordinator_;
    std::unique_ptr<dse::HttpServer> server_;
    std::thread server_thread_;
};

// ===========================================================================
// 1. Multiple simultaneous GET /search requests (different terms)
// ===========================================================================

TEST_F(HttpConcurrencyTest, MultipleSimultaneousSearchRequests)
{
    start_server();

    constexpr int kThreads = 8;
    std::vector<std::thread> threads;
    std::vector<std::pair<int, std::string>> results(kThreads);

    // Launch multiple threads each performing a different search.
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([this, i, &results]() {
            const std::string term = "term" + std::to_string(i + 1);
            results[i] = get("/search?q=" + term);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Every request should return 200 and find exactly one document.
    for (int i = 0; i < kThreads; ++i) {
        SCOPED_TRACE("thread " + std::to_string(i));
        EXPECT_EQ(results[i].first, 200);
        const auto j = nlohmann::json::parse(results[i].second);
        EXPECT_EQ(j["total"], 1u);
        EXPECT_EQ(j["results"][0]["document_id"], static_cast<unsigned>(i + 1));
    }
}

// ===========================================================================
// 2. Multiple simultaneous searches for the same term
// ===========================================================================

TEST_F(HttpConcurrencyTest, ConcurrentSearchesForSameTerm)
{
    start_server();

    constexpr int kThreads = 10;
    std::vector<std::thread> threads;
    std::vector<std::pair<int, std::string>> results(kThreads);

    // All threads search for the same term.
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([this, &results, i]() {
            results[i] = get("/search?q=term1");
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // All should return the same result.
    for (int i = 0; i < kThreads; ++i) {
        SCOPED_TRACE("thread " + std::to_string(i));
        EXPECT_EQ(results[i].first, 200);
        const auto j = nlohmann::json::parse(results[i].second);
        EXPECT_EQ(j["total"], 1u);
    }
}

// ===========================================================================
// 3. Concurrent GET /search and POST /documents (single request each)
// ===========================================================================

TEST_F(HttpConcurrencyTest, ConcurrentSearchAndIngestion)
{
    start_server();

    constexpr int kSearchers = 4;
    constexpr int kIngesters = 4;
    std::vector<std::thread> threads;

    std::atomic<int> successful_ingests{0};
    std::atomic<int> successful_searches{0};
    std::mutex mtx;
    std::vector<doc_id> ingested_ids;

    // Searcher threads — one request each.
    for (int i = 0; i < kSearchers; ++i) {
        threads.emplace_back([this, &successful_searches]() {
            auto [status, body] = get("/search?q=content");
            if (status == 200) {
                successful_searches.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    // Ingester threads — one request each.
    for (int i = 0; i < kIngesters; ++i) {
        threads.emplace_back([this, i, &successful_ingests, &mtx, &ingested_ids]() {
            const doc_id id = 1000 + i;
            const std::string body =
                R"({"id":)" + std::to_string(id) +
                R"(,"content":"concurrent doc })" + std::to_string(id) + R"("})";
            auto [status, resp] = post("/documents", body);
            if (status == 201) {
                successful_ingests.fetch_add(1, std::memory_order_relaxed);
                std::lock_guard lock(mtx);
                ingested_ids.push_back(id);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // All searches should have succeeded.
    EXPECT_EQ(successful_searches.load(), kSearchers);

    // All ingestion attempts should have succeeded (unique IDs).
    EXPECT_EQ(successful_ingests.load(), kIngesters);

    // Verify the ingested documents are searchable.
    for (doc_id id : ingested_ids) {
        auto [status, body] = get("/search?q=doc+" + std::to_string(id));
        EXPECT_EQ(status, 200);
        const auto j = nlohmann::json::parse(body);
        EXPECT_GE(j["total"].get<std::size_t>(), 1u);
    }
}

// ===========================================================================
// 4. Multiple concurrent POST /documents using unique IDs
// ===========================================================================

TEST_F(HttpConcurrencyTest, ConcurrentUniqueIngestion)
{
    start_server();

    constexpr int kThreads = 16;
    std::vector<std::thread> threads;
    std::vector<std::atomic<int>> status_codes(kThreads);
    for (auto& s : status_codes) s.store(0);

    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([this, i, &status_codes]() {
            const doc_id id = 2000 + i;
            const std::string body =
                R"({"id":)" + std::to_string(id) +
                R"(,"content":"unique doc })" + std::to_string(id) + R"("})";
            auto [status, resp] = post("/documents", body);
            status_codes[i].store(status);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // All 16 ingests should succeed (unique IDs).
    for (int i = 0; i < kThreads; ++i) {
        SCOPED_TRACE("thread " + std::to_string(i));
        EXPECT_EQ(status_codes[i].load(), 201);
    }
}

// ===========================================================================
// NOTE: Concurrent duplicate ID ingestion is a Phase 8B concern.
// IngestionService has a TOCTOU race: store_.contains() and store_.add()
// are separate lock acquisitions, so concurrent duplicate attempts can
// both pass the check and both call index_.add_document(), triggering
// the InvertedIndex assertion. Phase 8B will add service-level
// coordination to prevent this.
// ===========================================================================

// ===========================================================================
// 5. Search after concurrent ingestion sees all documents
// ===========================================================================

TEST_F(HttpConcurrencyTest, SearchAfterConcurrentIngestion)
{
    start_server();

    constexpr int kIngesters = 8;
    std::vector<std::thread> threads;

    // Phase 1: Ingest documents concurrently (one request per thread).
    for (int i = 0; i < kIngesters; ++i) {
        threads.emplace_back([this, i]() {
            const doc_id id = 4000 + i;
            const std::string body =
                R"({"id":)" + std::to_string(id) +
                R"(,"content":"findable word })" + std::to_string(id) + R"("})";
            post("/documents", body);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Phase 2: Verify all ingested documents are searchable.
    for (int i = 0; i < kIngesters; ++i) {
        const doc_id id = 4000 + i;
        auto [status, body] = get("/search?q=word+" + std::to_string(id));
        EXPECT_EQ(status, 200);
        const auto j = nlohmann::json::parse(body);
        EXPECT_GE(j["total"].get<std::size_t>(), 1u);
    }
}


// ===========================================================================
// Server remains responsive while concurrent requests are active
// ===========================================================================

TEST_F(HttpConcurrencyTest, ServerRemainsResponsive)
{
    start_server();

    // Launch background search requests.
    std::vector<std::thread> background;
    std::atomic<int> completed{0};

    for (int i = 0; i < 4; ++i) {
        background.emplace_back([this, &completed]() {
            get("/search?q=content&limit=100");
            completed.fetch_add(1, std::memory_order_relaxed);
        });
    }

    // While background requests are running, verify a simple request works.
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    auto [status, body] = get("/search?q=term1");
    EXPECT_EQ(status, 200);
    const auto j = nlohmann::json::parse(body);
    EXPECT_EQ(j["total"], 1u);

    for (auto& t : background) {
        t.join();
    }
    EXPECT_EQ(completed.load(), 4);
}

// ===========================================================================
// 6. Clean server shutdown after concurrent requests
// ===========================================================================

TEST_F(HttpConcurrencyTest, CleanShutdownAfterConcurrentRequests)
{
    start_server();

    constexpr int kThreads = 8;
    std::vector<std::thread> threads;

    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([this]() {
            get("/search?q=content");
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // All client threads done. TearDown will stop the server and join.
    // No crash, no hang — test completes successfully.
    SUCCEED();
}

// ===========================================================================
// 7. Mixed workload: readers and writers (single request each)
// ===========================================================================

TEST_F(HttpConcurrencyTest, MixedReadWriteWorkload)
{
    start_server();

    constexpr int kReaders = 6;
    constexpr int kWriters = 4;
    std::vector<std::thread> threads;

    std::atomic<int> total_reads{0};
    std::atomic<int> total_writes{0};

    // Readers — one request each.
    for (int i = 0; i < kReaders; ++i) {
        threads.emplace_back([this, &total_reads]() {
            auto [status, body] = get("/search?q=content&mode=or");
            if (status == 200) {
                total_reads.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    // Writers — one request each.
    for (int i = 0; i < kWriters; ++i) {
        threads.emplace_back([this, i, &total_writes]() {
            const doc_id id = 5000 + i;
            const std::string body =
                R"({"id":)" + std::to_string(id) +
                R"(,"content":"mixed workload doc })" + std::to_string(id) + R"("})";
            auto [status, resp] = post("/documents", body);
            if (status == 201) {
                total_writes.fetch_add(1, std::memory_order_relaxed);
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // All operations should have completed successfully.
    EXPECT_EQ(total_reads.load(), kReaders);
    EXPECT_EQ(total_writes.load(), kWriters);
}

// ===========================================================================
// 8. Concurrent AND and OR queries
// ===========================================================================

TEST_F(HttpConcurrencyTest, ConcurrentANDandORQueries)
{
    start_server();

    constexpr int kThreads = 8;
    std::vector<std::thread> threads;
    std::vector<std::pair<int, std::string>> results(kThreads);

    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([this, i, &results]() {
            if (i % 2 == 0) {
                results[i] = get("/search?q=content&mode=or");
            } else {
                results[i] = get("/search?q=document+content&mode=and");
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    for (int i = 0; i < kThreads; ++i) {
        SCOPED_TRACE("thread " + std::to_string(i));
        EXPECT_EQ(results[i].first, 200);
        const auto j = nlohmann::json::parse(results[i].second);
        if (i % 2 == 0) {
            EXPECT_EQ(j["mode"], "or");
            EXPECT_GE(j["total"].get<std::size_t>(), 1u);
        } else {
            EXPECT_EQ(j["mode"], "and");
            EXPECT_GE(j["total"].get<std::size_t>(), 1u);
        }
    }
}

} // namespace
