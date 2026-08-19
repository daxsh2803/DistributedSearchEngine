// Distributed Search Engine - HTTP API Integration Tests (Phase 5B-2).
//
// Real HTTP requests against a localhost server using cpp-httplib's
// client. Tests verify the full stack: HTTP transport → SearchService
// → Ranker → InvertedIndex.
//
// Lifecycle:
//   - Each test starts an HttpServer on an OS-assigned port (port 0).
//   - The server runs in a background thread.
//   - httplib::Client sends real HTTP GET /search requests.
//   - After assertions, the server is stopped and the thread is joined.

#include <gtest/gtest.h>

#include "http_server.h"
#include "inverted_index.h"
#include "ranker.h"
#include "search_service.h"
#include "tokenizer.h"

#include <httplib.h>
#include <nlohmann/json.hpp>

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <thread>
#include <vector>

namespace {

using dse::InvertedIndex;
using dse::SearchMode;
using dse::SearchService;
using dse::doc_id;

// Build an InvertedIndex from a list of (id, text) pairs.
InvertedIndex build_index(
    std::initializer_list<std::pair<doc_id, std::string_view>> docs)
{
    InvertedIndex index;
    for (const auto& [id, text] : docs) {
        index.add_document(id, text);
    }
    return index;
}

// Test fixture: starts an HTTP server per test, tears it down after.
class HttpApiTest : public ::testing::Test {
protected:
    void SetUp() override {
        index_ = build_index({
            {1, "the quick brown fox"},
            {2, "the lazy dog"},
            {3, "the quick brown dog"},
            {4, "the fox and the dog"},
        });
        service_ = std::make_unique<SearchService>(index_);
        server_  = std::make_unique<dse::HttpServer>(*service_);
    }

    void start_server() {
        server_thread_ = std::thread([this]() {
            server_->listen(0);  // OS-assigned port
        });
        // Block until the server is actually accepting connections.
        server_->wait_until_ready();
    }

    void TearDown() override {
        server_->stop();
        if (server_thread_.joinable()) {
            server_thread_.join();
        }
    }

    // Convenience: make a GET request and return the response body + status.
    std::pair<int, std::string> get(const std::string& path) {
        httplib::Client client("localhost", server_->port());
        client.set_connection_timeout(5);
        client.set_read_timeout(5);
        auto res = client.Get(path);
        if (!res) {
            return {0, ""};
        }
        return {res->status, res->body};
    }

    InvertedIndex index_;
    std::unique_ptr<SearchService> service_;
    std::unique_ptr<dse::HttpServer> server_;
    std::thread server_thread_;
};

// ===========================================================================
// 1. Basic OR search (default mode)
// ===========================================================================

TEST_F(HttpApiTest, DefaultORSearchReturnsMatchingDocuments)
{
    start_server();
    const auto [status, body] = get("/search?q=fox");

    EXPECT_EQ(status, 200);

    const auto j = nlohmann::json::parse(body);
    EXPECT_EQ(j["query"], "fox");
    EXPECT_EQ(j["mode"], "or");
    EXPECT_EQ(j["total"], 2u);  // docs 1 and 4 contain "fox"

    const auto& results = j["results"];
    EXPECT_EQ(results.size(), 2u);
    EXPECT_EQ(results[0]["document_id"], 1);
    EXPECT_EQ(results[1]["document_id"], 4);
}

// ===========================================================================
// 2. Explicit AND search
// ===========================================================================

TEST_F(HttpApiTest, ExplicitANDSearchReturnsIntersection)
{
    start_server();
    const auto [status, body] = get("/search?q=quick+fox&mode=and");

    EXPECT_EQ(status, 200);

    const auto j = nlohmann::json::parse(body);
    EXPECT_EQ(j["mode"], "and");
    EXPECT_EQ(j["total"], 1u);  // only doc 1 has both "quick" and "fox"
    EXPECT_EQ(j["results"][0]["document_id"], 1);
}

// ===========================================================================
// 3. Explicit limit
// ===========================================================================

TEST_F(HttpApiTest, ExplicitLimitTruncatesResults)
{
    start_server();
    const auto [status, body] = get("/search?q=the&limit=2");

    EXPECT_EQ(status, 200);

    const auto j = nlohmann::json::parse(body);
    EXPECT_EQ(j["limit"], 2u);
    EXPECT_EQ(j["total"], 4u);     // all 4 docs contain "the"
    EXPECT_EQ(j["results"].size(), 2u);  // but only 2 returned
}

// ===========================================================================
// 4. Missing q parameter
// ===========================================================================

TEST_F(HttpApiTest, MissingQueryParameterReturns400)
{
    start_server();
    const auto [status, body] = get("/search");

    EXPECT_EQ(status, 400);

    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.contains("error"));
}

// ===========================================================================
// 5. Empty q parameter
// ===========================================================================

TEST_F(HttpApiTest, EmptyQueryParameterReturns400)
{
    start_server();
    const auto [status, body] = get("/search?q=");

    EXPECT_EQ(status, 400);

    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.contains("error"));
}

// ===========================================================================
// 6. Invalid mode
// ===========================================================================

TEST_F(HttpApiTest, InvalidModeReturns400)
{
    start_server();
    const auto [status, body] = get("/search?q=fox&mode=xor");

    EXPECT_EQ(status, 400);

    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.contains("error"));
}

// ===========================================================================
// 7. Invalid limit — out of range
// ===========================================================================

TEST_F(HttpApiTest, InvalidLimitOutOfRangeReturns400)
{
    start_server();
    const auto [status, body] = get("/search?q=fox&limit=0");

    EXPECT_EQ(status, 400);

    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.contains("error"));
}

// ===========================================================================
// 7b. Invalid limit — non-numeric
// ===========================================================================

TEST_F(HttpApiTest, InvalidLimitNonNumericReturns400)
{
    start_server();
    const auto [status, body] = get("/search?q=fox&limit=abc");

    EXPECT_EQ(status, 400);

    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.contains("error"));
}

// ===========================================================================
// 8. JSON response structure
// ===========================================================================

TEST_F(HttpApiTest, ResponseHasCorrectJsonStructure)
{
    start_server();
    const auto [status, body] = get("/search?q=dog&limit=1");

    EXPECT_EQ(status, 200);

    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.is_object());
    EXPECT_TRUE(j.contains("query"));
    EXPECT_TRUE(j.contains("mode"));
    EXPECT_TRUE(j.contains("total"));
    EXPECT_TRUE(j.contains("limit"));
    EXPECT_TRUE(j.contains("results"));

    EXPECT_TRUE(j["results"].is_array());
    EXPECT_TRUE(j["results"][0].contains("document_id"));
    EXPECT_TRUE(j["results"][0].contains("score"));
}

// ===========================================================================
// 9. Content-Type is application/json
// ===========================================================================

TEST_F(HttpApiTest, ContentTypeIsApplicationJson)
{
    start_server();
    httplib::Client client("localhost", server_->port());

    auto res = client.Get("/search?q=fox");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 200);
    EXPECT_NE(res->get_header_value("Content-Type").find("application/json"),
              std::string::npos);
}

// ===========================================================================
// 10. Empty search results
// ===========================================================================

TEST_F(HttpApiTest, NoMatchingDocumentsReturnsEmptyResults)
{
    start_server();
    const auto [status, body] = get("/search?q=elephant");

    EXPECT_EQ(status, 200);

    const auto j = nlohmann::json::parse(body);
    EXPECT_EQ(j["total"], 0u);
    EXPECT_TRUE(j["results"].empty());
}

// ===========================================================================
// 11. Ranked results in correct order
// ===========================================================================

TEST_F(HttpApiTest, ResultsAreRankedByScoreDescending)
{
    start_server();
    const auto [status, body] = get("/search?q=the&mode=or");

    EXPECT_EQ(status, 200);

    const auto j = nlohmann::json::parse(body);
    ASSERT_GE(j["results"].size(), 2u);

    // Verify scores are in descending order.
    double prev_score = j["results"][0]["score"].get<double>();
    for (std::size_t i = 1; i < j["results"].size(); ++i) {
        const double cur = j["results"][i]["score"].get<double>();
        EXPECT_GE(prev_score, cur);
        prev_score = cur;
    }
}

// ===========================================================================
// 12. Multiple sequential requests to the same server
// ===========================================================================

TEST_F(HttpApiTest, MultipleRequestsToSameServer)
{
    start_server();

    // First request
    const auto [s1, b1] = get("/search?q=fox");
    EXPECT_EQ(s1, 200);
    const auto j1 = nlohmann::json::parse(b1);
    EXPECT_EQ(j1["total"], 2u);

    // Second request — different query
    const auto [s2, b2] = get("/search?q=dog");
    EXPECT_EQ(s2, 200);
    const auto j2 = nlohmann::json::parse(b2);
    EXPECT_EQ(j2["total"], 3u);  // docs 2, 3, 4

    // Third request — AND mode
    const auto [s3, b3] = get("/search?q=quick+dog&mode=and");
    EXPECT_EQ(s3, 200);
    const auto j3 = nlohmann::json::parse(b3);
    EXPECT_EQ(j3["total"], 1u);  // only doc 3
}

// ===========================================================================
// 13. Score values are valid floating-point numbers
// ===========================================================================

TEST_F(HttpApiTest, ScoresAreValidDoubles)
{
    start_server();
    const auto [status, body] = get("/search?q=fox");

    EXPECT_EQ(status, 200);

    const auto j = nlohmann::json::parse(body);
    for (const auto& r : j["results"]) {
        EXPECT_TRUE(r["score"].is_number());
        EXPECT_GE(r["score"].get<double>(), 0.0);
    }
}

// ===========================================================================
// 14. Limit at maximum (100) works
// ===========================================================================

TEST_F(HttpApiTest, LargeLimitReturnsAllResults)
{
    start_server();
    const auto [status, body] = get("/search?q=the&limit=100");

    EXPECT_EQ(status, 200);

    const auto j = nlohmann::json::parse(body);
    EXPECT_EQ(j["results"].size(), j["total"]);
}

// ===========================================================================
// 15. Limit above 100 returns 400
// ===========================================================================

TEST_F(HttpApiTest, LimitAbove100Returns400)
{
    start_server();
    const auto [status, body] = get("/search?q=the&limit=101");

    EXPECT_EQ(status, 400);
}

// ===========================================================================
// 16. Query with special characters (URL-encoded)
// ===========================================================================

TEST_F(HttpApiTest, QueryWithSpecialCharactersHandled)
{
    start_server();
    // Search for "the" — should work even if other chars in URL
    const auto [status, body] = get("/search?q=the%20quick");

    EXPECT_EQ(status, 200);

    const auto j = nlohmann::json::parse(body);
    EXPECT_EQ(j["query"], "the quick");
}

// ===========================================================================
// 17. Error response has correct structure
// ===========================================================================

TEST_F(HttpApiTest, ErrorResponseHasErrorField)
{
    start_server();
    const auto [status, body] = get("/search");

    EXPECT_EQ(status, 400);

    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.contains("error"));
    EXPECT_TRUE(j["error"].is_string());
    EXPECT_FALSE(j["error"].get<std::string>().empty());
}

// ===========================================================================
// 18. Document IDs in results are unsigned integers
// ===========================================================================

TEST_F(HttpApiTest, DocumentIdsAreUnsignedIntegers)
{
    start_server();
    const auto [status, body] = get("/search?q=fox");

    EXPECT_EQ(status, 200);

    const auto j = nlohmann::json::parse(body);
    for (const auto& r : j["results"]) {
        EXPECT_TRUE(r["document_id"].is_number_unsigned());
    }
}

} // namespace
