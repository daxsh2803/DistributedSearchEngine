// Distributed Search Engine - Application Integration Test (Phase 5B-3).
//
// Starts the real HTTP server with the seed corpus and verifies that
// GET /search returns valid JSON results. This is a lightweight
// application-level test — it does NOT duplicate the 19 HTTP API tests.

#include <gtest/gtest.h>

#include "http_server.h"
#include "inverted_index.h"
#include "search_service.h"
#include "tokenizer.h"

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>
#include <chrono>

namespace {

using dse::InvertedIndex;
using dse::SearchService;
using dse::doc_id;

// Build the seed corpus identical to main.cpp.
void load_seed_corpus(InvertedIndex& index)
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
        index.add_document(doc.id, doc.text);
    }
}

// Test fixture: starts server with seed corpus, tears down after.
class AppIntegrationTest : public ::testing::Test {
protected:
    void SetUp() override {
        load_seed_corpus(index_);
        service_ = std::make_unique<SearchService>(index_);
        server_  = std::make_unique<dse::HttpServer>(*service_);
    }

    void start_server() {
        server_thread_ = std::thread([this]() {
            server_->listen(0);  // OS-assigned port
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

    InvertedIndex index_;
    std::unique_ptr<SearchService> service_;
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

} // namespace
