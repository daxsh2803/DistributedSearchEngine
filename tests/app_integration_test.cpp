// Distributed Search Engine - Application Integration Test (Phase 5B-3, Phase 10).
//
// Starts the real HTTP server with the seed corpus and verifies that
// GET /search returns valid JSON results. Uses ShardCoordinator with
// shard_count=1 for backward compatibility.

#include <gtest/gtest.h>

#include "document_store.h"
#include "http_server.h"
#include "ingestion_service.h"
#include "inverted_index.h"
#include "search_service.h"
#include "shard.h"
#include "shard_coordinator.h"
#include "shard_router.h"
#include "tokenizer.h"

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <memory>
#include <string>
#include <thread>
#include <chrono>
#include <vector>

namespace {

using dse::DocumentStore;
using dse::IngestionService;
using dse::InvertedIndex;
using dse::SearchService;
using dse::doc_id;
using dse::Shard;
using dse::ShardCoordinator;
using dse::ShardRouter;

// Build the seed corpus identical to main.cpp via coordinator.
void load_seed_corpus(ShardCoordinator& coord)
{
    struct Doc {
        doc_id id;
        std::string_view text;
    };

    const std::vector<Doc> docs = {
        {  1, "The quick brown fox jumps over the lazy dog" },
        {  2, "A fast red fox leaps over a sleeping hound" },
        {  3, "The quick brown dog chases the lazy fox" },
        {  4, "C++ is a high-performance systems programming language" },
        {  5, "Rust is a systems language focused on memory safety" },
        {  6, "Python is a versatile scripting language" },
        {  7, "The Linux kernel is written in C" },
        {  8, "Git is a distributed version control system" },
        {  9, "Docker containers package applications for deployment" },
        { 10, "A search engine indexes documents for fast retrieval" },
        { 11, "Inverted maps map terms to document lists" },
        { 12, "TF-IDF scores term importance across documents" },
        { 13, "PostgreSQL is a relational database management system" },
        { 14, "Redis is an in-memory key-value store" },
        { 15, "Kafka handles high-throughput event streaming" },
        { 16, "Kubernetes orchestrates containerized applications" },
        { 17, "Machine learning models learn patterns from data" },
        { 18, "Neural networks are inspired by biological neurons" },
        { 19, "Web browsers render HTML and execute JavaScript" },
        { 20, "HTTP is the foundation of data communication on the web" },
    };

    for (const auto& doc : docs) {
        coord.ingest({doc.id, std::string(doc.text)});
    }
}

// Rebuild a shard's index from its DocumentStore.
void rebuild_index(InvertedIndex& index, const DocumentStore& store)
{
    for (const auto& [id, doc] : store.all()) {
        index.add_document(id, doc.content);
    }
}

// Build seed corpus directly into an InvertedIndex (for persistence tests).
void load_seed_corpus_direct(InvertedIndex& index)
{
    struct Doc { doc_id id; std::string_view text; };
    const std::vector<Doc> docs = {
        {1, "The quick brown fox jumps over the lazy dog"},
        {2, "A fast red fox leaps over a sleeping hound"},
        {3, "The quick brown dog chases the lazy fox"},
        {4, "C++ is a high-performance systems programming language"},
        {5, "Rust is a systems language focused on memory safety"},
        {6, "Python is a versatile scripting language"},
        {7, "The Linux kernel is written in C"},
        {8, "Git is a distributed version control system"},
        {9, "Docker containers package applications for deployment"},
        {10, "A search engine indexes documents for fast retrieval"},
        {11, "Inverted maps map terms to document lists"},
        {12, "TF-IDF scores term importance across documents"},
        {13, "PostgreSQL is a relational database management system"},
        {14, "Redis is an in-memory key-value store"},
        {15, "Kafka handles high-throughput event streaming"},
        {16, "Kubernetes orchestrates containerized applications"},
        {17, "Machine learning models learn patterns from data"},
        {18, "Neural networks are inspired by biological neurons"},
        {19, "Web browsers render HTML and execute JavaScript"},
        {20, "HTTP is the foundation of data communication on the web"},
    };
    for (const auto& doc : docs) {
        index.add_document(doc.id, doc.text);
    }
}

// Test fixture: starts server with seed corpus, tears down after.
class AppIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto router = std::make_unique<ShardRouter>(1);
        std::vector<std::unique_ptr<Shard>> shards;
        shards.push_back(std::make_unique<Shard>());
        coordinator_ = std::make_unique<ShardCoordinator>(
            std::move(router), std::move(shards));
        load_seed_corpus(*coordinator_);
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
// 1. Single-term search returns matching documents
// ===========================================================================

TEST_F(AppIntegrationTest, SingleTermSearchReturnsResults)
{
    start_server();
    const auto [status, body] = get("/search?q=fox");

    EXPECT_EQ(status, 200);
    const auto j = nlohmann::json::parse(body);
    EXPECT_EQ(j["query"], "fox");
    EXPECT_GE(j["total"].get<std::size_t>(), 2u);
    EXPECT_TRUE(j["results"].is_array());
    EXPECT_FALSE(j["results"].empty());
}

// ===========================================================================
// 2. Multi-term AND search
// ===========================================================================

TEST_F(AppIntegrationTest, MultiTermANDSearch)
{
    start_server();
    const auto [status, body] = get("/search?q=quick+fox&mode=and");

    EXPECT_EQ(status, 200);
    const auto j = nlohmann::json::parse(body);
    EXPECT_EQ(j["mode"], "and");
    EXPECT_GE(j["total"].get<std::size_t>(), 1u);
    // doc 1 has both "quick" and "fox"
}

// ===========================================================================
// 3. Multi-term OR search with ranking
// ===========================================================================

TEST_F(AppIntegrationTest, MultiTermORSearchWithRanking)
{
    start_server();
    const auto [status, body] = get("/search?q=c++&mode=or");

    EXPECT_EQ(status, 200);
    const auto j = nlohmann::json::parse(body);
    EXPECT_GE(j["total"].get<std::size_t>(), 1u);

    // Verify scores are in descending order
    if (j["results"].size() >= 2) {
        const double first_score = j["results"][0]["score"].get<double>();
        const double second_score = j["results"][1]["score"].get<double>();
        EXPECT_GE(first_score, second_score);
    }
}

// ===========================================================================
// 4. Valid JSON structure
// ===========================================================================

TEST_F(AppIntegrationTest, ResponseIsValidJSON)
{
    start_server();
    const auto [status, body] = get("/search?q=search");

    EXPECT_EQ(status, 200);

    // Must parse without exception
    const auto j = nlohmann::json::parse(body);

    EXPECT_TRUE(j.is_object());
    EXPECT_TRUE(j.contains("query"));
    EXPECT_TRUE(j.contains("mode"));
    EXPECT_TRUE(j.contains("total"));
    EXPECT_TRUE(j.contains("limit"));
    EXPECT_TRUE(j.contains("results"));
    EXPECT_TRUE(j["results"].is_array());
}

// ===========================================================================
// 5. Application process starts and serves correctly
// ===========================================================================

TEST_F(AppIntegrationTest, ApplicationStartsAndServes)
{
    start_server();
    EXPECT_GT(server_->port(), 0);

    // Basic connectivity check
    const auto [status, body] = get("/search?q=engine");
    EXPECT_EQ(status, 200);
    EXPECT_FALSE(body.empty());
}

// ===========================================================================
// 6. POST /documents ingestion via HTTP
// ===========================================================================

TEST_F(AppIntegrationTest, DocumentIngestionViaHTTP)
{
    start_server();

    // Ingest a new document via POST /documents
    const auto [s1, b1] = post(
        "/documents",
        R"({"id": 100, "content": "phase six ingestion test"})");
    EXPECT_EQ(s1, 201);

    const auto j1 = nlohmann::json::parse(b1);
    EXPECT_EQ(j1["document_id"], 100u);
    EXPECT_GE(j1["terms_indexed"].get<std::size_t>(), 1u);
}

// ===========================================================================
// 7. End-to-end: ingest → search round-trip
// ===========================================================================

TEST_F(AppIntegrationTest, IngestThenSearchRoundTrip)
{
    start_server();

    // Ingest a document with a unique term that won't match seed corpus
    const auto [s1, b1] = post(
        "/documents",
        R"({"id": 200, "content": "xyzzy plugh"})");
    EXPECT_EQ(s1, 201);

    // Search for that unique term — should find exactly doc 200
    const auto [s2, b2] = get("/search?q=xyzzy");
    EXPECT_EQ(s2, 200);

    const auto j2 = nlohmann::json::parse(b2);
    EXPECT_EQ(j2["total"], 1u);
    EXPECT_EQ(j2["results"][0]["document_id"], 200u);
}

// ===========================================================================
// 8. POST /documents duplicate returns 409
// ===========================================================================

TEST_F(AppIntegrationTest, DuplicateDocumentIngestionReturns409)
{
    start_server();

    // First ingestion succeeds
    const auto [s1, b1] = post(
        "/documents",
        R"({"id": 300, "content": "first ingestion"})");
    EXPECT_EQ(s1, 201);

    // Duplicate returns 409
    const auto [s2, b2] = post(
        "/documents",
        R"({"id": 300, "content": "second ingestion"})");
    EXPECT_EQ(s2, 409);
}

// ===========================================================================
// 9. Existing search behavior preserved after ingestion
// ===========================================================================

TEST_F(AppIntegrationTest, ExistingSearchPreservedAfterIngestion)
{
    start_server();

    // Ingest a new document
    post("/documents",
         R"({"id": 500, "content": "brand new content"})");

    // Original seed corpus search still works
    const auto [s1, b1] = get("/search?q=fox&mode=or");
    EXPECT_EQ(s1, 200);
    const auto j1 = nlohmann::json::parse(b1);
    EXPECT_GE(j1["total"].get<std::size_t>(), 2u);

    // AND mode still works — doc 1 (quick brown fox) and doc 3 (quick brown dog...lazy fox)
    const auto [s2, b2] = get("/search?q=quick+fox&mode=and");
    EXPECT_EQ(s2, 200);
    const auto j2 = nlohmann::json::parse(b2);
    EXPECT_EQ(j2["total"], 2u);  // docs 1 and 3 both have "quick" and "fox"
}

} // namespace

// ===========================================================================
// Phase 7A-2: Persistence Integration Tests
// ===========================================================================

namespace {

std::string persist_temp_path(const std::string& name)
{
    return std::tmpnam(nullptr) + std::string("_") + name + ".jsonl";
}

struct PersistTempFile {
    std::string path;
    ~PersistTempFile() { std::remove(path.c_str()); }
};

} // namespace

// ---------------------------------------------------------------------------
// 10. Startup with existing persistence file
// ---------------------------------------------------------------------------

TEST(PersistenceStartup, LoadedDocumentBecomesSearchable)
{
    const auto path = persist_temp_path("startup");
    PersistTempFile guard{path};

    // Phase 1: Ingest a unique document, let it persist.
    {
        dse::InvertedIndex index;
        dse::DocumentStore store;
        dse::IngestionService ingestion(index, store, path);

        // Ingest a unique document
        const auto resp = ingestion.ingest({9000, "unique persistence test"});
        EXPECT_FALSE(resp.is_error);

        // Verify it's in the store
        EXPECT_TRUE(store.contains(9000));
    }
    // Services destroyed here.

    // Phase 2: Start fresh, load from persistence, rebuild index.
    {
        dse::InvertedIndex index;
        dse::DocumentStore store;
        EXPECT_TRUE(store.load(path));  // Load persisted documents
        rebuild_index(index, store);     // Rebuild index from documents
        const dse::SearchService search(index);

        // The persisted document should be searchable
        EXPECT_EQ(store.size(), 1u);
        EXPECT_EQ(index.document_count(), 1u);

        const auto results = search.search({"unique", dse::SearchMode::Or, 10});
        EXPECT_EQ(results.total, 1u);
        EXPECT_EQ(results.results[0].document_id, 9000u);
    }
}

// ---------------------------------------------------------------------------
// 11. First run with missing file loads seed corpus
// ---------------------------------------------------------------------------

TEST(PersistenceStartup, MissingFileUsesSeedCorpus)
{
    const auto path = persist_temp_path("nofile");
    // No guard — file doesn't exist

    dse::InvertedIndex index;
    dse::DocumentStore store;

    // load() returns false when file doesn't exist
    EXPECT_FALSE(store.load(path));

    // Store should be empty
    EXPECT_EQ(store.size(), 0u);

    // Load seed corpus directly into the index
    load_seed_corpus_direct(index);

    // Seed corpus loaded
    EXPECT_EQ(index.document_count(), 20u);
}

// ---------------------------------------------------------------------------
// 12. Newly ingested document survives restart
// ---------------------------------------------------------------------------

TEST(PersistenceStartup, IngestedDocumentSurvivesRestart)
{
    const auto path = persist_temp_path("survive");
    PersistTempFile guard{path};

    // Session 1: ingest a document
    {
        dse::InvertedIndex index;
        dse::DocumentStore store;
        dse::IngestionService ingestion(index, store, path);
        const dse::SearchService search(index);

        const auto resp = ingestion.ingest({7777, "survives the restart"});
        EXPECT_FALSE(resp.is_error);
    }

    // Session 2: load from persistence
    {
        dse::InvertedIndex index;
        dse::DocumentStore store;
        EXPECT_TRUE(store.load(path));
        rebuild_index(index, store);

        EXPECT_TRUE(store.contains(7777));
        const auto doc = store.get(7777);
        ASSERT_TRUE(doc.has_value());
        EXPECT_EQ(doc->content, "survives the restart");
        EXPECT_EQ(index.document_count(), 1u);
    }
}

// ---------------------------------------------------------------------------
// 13. Malformed persistence records follow documented policy
// ---------------------------------------------------------------------------

TEST(PersistenceStartup, MalformedRecordsSkippedWithWarning)
{
    const auto path = persist_temp_path("malformed");
    PersistTempFile guard{path};

    // Write a file with some corrupt lines
    {
        std::ofstream ofs(path);
        ofs << R"({"id": 1, "content": "good one"})" << "\n";
        ofs << "this is not json\n";
        ofs << R"({"id": 2, "content": "good two"})" << "\n";
    }

    dse::InvertedIndex index;
    dse::DocumentStore store;

    // load() returns true (file opened), even with corrupt lines
    EXPECT_TRUE(store.load(path));

    // Only valid records loaded
    EXPECT_EQ(store.size(), 2u);
    rebuild_index(index, store);
    EXPECT_EQ(index.document_count(), 2u);
}

// ---------------------------------------------------------------------------
// 14. Persistence failure does not return false success
// ---------------------------------------------------------------------------

TEST(PersistenceStartup, PersistenceFailureReportsError)
{
    dse::InvertedIndex index;
    dse::DocumentStore store;

    // Use a path that cannot be written to (invalid directory)
    dse::IngestionService ingestion(index, store, "/nonexistent/dir/file.jsonl");

    const auto resp = ingestion.ingest({1, "test"});

    // The document IS indexed in memory (best-effort)
    EXPECT_TRUE(store.contains(1));
    EXPECT_EQ(index.document_count(), 1u);

    // But the response reports the persistence failure
    EXPECT_TRUE(resp.is_error);
    EXPECT_NE(resp.error_message.find("persist"), std::string::npos);
}

// ---------------------------------------------------------------------------
// 15. Existing Phase 5 search behavior remains intact
// ---------------------------------------------------------------------------

TEST(PersistenceStartup, SearchBehaviorUnchanged)
{
    const auto path = persist_temp_path("search");
    PersistTempFile guard{path};

    // Load seed corpus directly into the index
    dse::InvertedIndex index;
    dse::DocumentStore store;
    load_seed_corpus_direct(index);

    // AND mode works
    const dse::SearchService search(index);
    const auto r1 = search.search({"quick fox", dse::SearchMode::And, 10});
    EXPECT_GE(r1.total, 1u);

    // OR mode works
    const auto r2 = search.search({"fox", dse::SearchMode::Or, 10});
    EXPECT_GE(r2.total, 2u);
}

// ---------------------------------------------------------------------------
// 16. Rebuild index from DocumentStore
// ---------------------------------------------------------------------------

TEST(PersistenceStartup, IndexRebuiltFromDocumentStore)
{
    const auto path = persist_temp_path("rebuild");
    PersistTempFile guard{path};

    // Create and persist some documents
    {
        dse::InvertedIndex index;
        dse::DocumentStore store;
        dse::IngestionService ingestion(index, store, path);
        ingestion.ingest({1, "alpha beta"});
        ingestion.ingest({2, "beta gamma"});
    }

    // Create a fresh index and rebuild from loaded documents
    {
        dse::InvertedIndex index;
        dse::DocumentStore store;
        EXPECT_TRUE(store.load(path));

        // Index is empty before rebuild
        EXPECT_EQ(index.document_count(), 0u);

        // Rebuild from DocumentStore
        rebuild_index(index, store);

        // Index now has the documents
        EXPECT_EQ(index.document_count(), 2u);
        EXPECT_EQ(index.term_count(), 3u);  // alpha, beta, gamma

        // Search works
        const dse::SearchService search(index);
        const auto results = search.search({"beta", dse::SearchMode::Or, 10});
        EXPECT_EQ(results.total, 2u);
    }
}
