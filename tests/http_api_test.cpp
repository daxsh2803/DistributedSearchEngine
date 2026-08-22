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

#include "document_store.h"
#include "http_server.h"
#include "ingestion_service.h"
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

using dse::DocumentStore;
using dse::IngestionService;
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
        // IngestionService is not used in these tests but required by HttpServer
        ingestion_ = std::make_unique<IngestionService>(index_, store_);
        server_  = std::make_unique<dse::HttpServer>(*service_, *ingestion_);
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

    // Convenience: make a POST request with a JSON body.
    std::pair<int, std::string> post(const std::string& path,
                                     const std::string& json_body) {
        httplib::Client client("localhost", server_->port());
        client.set_connection_timeout(5);
        client.set_read_timeout(5);
        auto res = client.Post(path.c_str(),
                               json_body.c_str(),
                               "application/json");
        if (!res) {
            return {0, ""};
        }
        return {res->status, res->body};
    }

    // Convenience: make a PUT request with a JSON body.
    std::pair<int, std::string> put(const std::string& path,
                                    const std::string& json_body) {
        httplib::Client client("localhost", server_->port());
        client.set_connection_timeout(5);
        client.set_read_timeout(5);
        auto res = client.Put(path.c_str(),
                              json_body.c_str(),
                              "application/json");
        if (!res) {
            return {0, ""};
        }
        return {res->status, res->body};
    }

    // Convenience: make a DELETE request.
    std::pair<int, std::string> del(const std::string& path) {
        httplib::Client client("localhost", server_->port());
        client.set_connection_timeout(5);
        client.set_read_timeout(5);
        auto res = client.Delete(path.c_str());
        if (!res) {
            return {0, ""};
        }
        return {res->status, res->body};
    }

    InvertedIndex index_;
    DocumentStore store_;
    std::unique_ptr<SearchService> service_;
    std::unique_ptr<dse::IngestionService> ingestion_;
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

// ===========================================================================
// POST /documents — Phase 6 Ingestion Integration Tests
// ===========================================================================

// ===========================================================================
// 19. Valid document ingestion returns 201
// ===========================================================================

TEST_F(HttpApiTest, ValidDocumentIngestionReturns201)
{
    start_server();
    const auto [status, body] = post(
        "/documents",
        R"({"id": 100, "content": "The quick brown fox"})");

    EXPECT_EQ(status, 201);

    const auto j = nlohmann::json::parse(body);
    EXPECT_EQ(j["document_id"], 100u);
    EXPECT_GE(j["terms_indexed"].get<std::size_t>(), 1u);
}

// ===========================================================================
// 20. Ingested document is immediately searchable
// ===========================================================================

TEST_F(HttpApiTest, IngestedDocumentIsImmediatelySearchable)
{
    start_server();

    // Ingest a unique document
    const auto [ingest_status, ingest_body] = post(
        "/documents",
        R"({"id": 200, "content": "unique_term_xyz"})");
    EXPECT_EQ(ingest_status, 201);

    // Search for the unique term — should find exactly doc 200
    const auto [search_status, search_body] = get("/search?q=unique_term_xyz");
    EXPECT_EQ(search_status, 200);

    const auto j = nlohmann::json::parse(search_body);
    EXPECT_EQ(j["total"], 1u);
    EXPECT_EQ(j["results"][0]["document_id"], 200u);
    EXPECT_GT(j["results"][0]["score"].get<double>(), 0.0);
}

// ===========================================================================
// 21. Duplicate document ID returns 409
// ===========================================================================

TEST_F(HttpApiTest, DuplicateDocumentIdReturns409)
{
    start_server();

    // First ingestion — should succeed
    const auto [s1, b1] = post(
        "/documents",
        R"({"id": 300, "content": "original content"})");
    EXPECT_EQ(s1, 201);

    // Second ingestion with same ID — should return 409
    const auto [s2, b2] = post(
        "/documents",
        R"({"id": 300, "content": "attempted overwrite"})");
    EXPECT_EQ(s2, 409);

    const auto j = nlohmann::json::parse(b2);
    EXPECT_TRUE(j.contains("error"));
}

// ===========================================================================
// 22. Duplicate ingestion does not modify index
// ===========================================================================

TEST_F(HttpApiTest, DuplicateIngestionDoesNotModifyIndex)
{
    start_server();

    // Ingest document with known content
    post("/documents",
         R"({"id": 400, "content": "alpha beta"})");

    // Search for "alpha" — should find doc 400
    const auto [s1, b1] = get("/search?q=alpha");
    const auto j1 = nlohmann::json::parse(b1);
    EXPECT_EQ(j1["total"], 1u);
    EXPECT_EQ(j1["results"][0]["document_id"], 400u);

    // Attempt duplicate ingestion
    post("/documents",
         R"({"id": 400, "content": "completely different"})");

    // Search for original content still works
    const auto [s2, b2] = get("/search?q=alpha");
    const auto j2 = nlohmann::json::parse(b2);
    EXPECT_EQ(j2["total"], 1u);
    EXPECT_EQ(j2["results"][0]["document_id"], 400u);

    // Search for new content does NOT find it
    const auto [s3, b3] = get("/search?q=completely");
    const auto j3 = nlohmann::json::parse(b3);
    EXPECT_EQ(j3["total"], 0u);
}

// ===========================================================================
// 23. Empty content returns 400
// ===========================================================================

TEST_F(HttpApiTest, EmptyContentReturns400)
{
    start_server();
    const auto [status, body] = post(
        "/documents",
        R"({"id": 500, "content": ""})");

    EXPECT_EQ(status, 400);

    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.contains("error"));
}

// ===========================================================================
// 24. Whitespace-only content returns 400
// ===========================================================================

TEST_F(HttpApiTest, WhitespaceOnlyContentReturns400)
{
    start_server();
    const auto [status, body] = post(
        "/documents",
        R"({"id": 600, "content": "   \t\n  "})");

    EXPECT_EQ(status, 400);

    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.contains("error"));
}

// ===========================================================================
// 25. Malformed JSON returns 400
// ===========================================================================

TEST_F(HttpApiTest, MalformedJsonReturns400)
{
    start_server();
    const auto [status, body] = post(
        "/documents",
        R"({invalid json})");

    EXPECT_EQ(status, 400);

    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.contains("error"));
}

// ===========================================================================
// 26. Missing id field returns 400
// ===========================================================================

TEST_F(HttpApiTest, MissingIdFieldReturns400)
{
    start_server();
    const auto [status, body] = post(
        "/documents",
        R"({"content": "some text"})");

    EXPECT_EQ(status, 400);

    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.contains("error"));
}

// ===========================================================================
// 27. Missing content field returns 400
// ===========================================================================

TEST_F(HttpApiTest, MissingContentFieldReturns400)
{
    start_server();
    const auto [status, body] = post(
        "/documents",
        R"({"id": 700})");

    EXPECT_EQ(status, 400);

    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.contains("error"));
}

// ===========================================================================
// 28. Invalid document ID (negative) returns 400
// ===========================================================================

TEST_F(HttpApiTest, NegativeDocumentIdReturns400)
{
    start_server();
    const auto [status, body] = post(
        "/documents",
        R"({"id": -1, "content": "test"})");

    EXPECT_EQ(status, 400);

    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.contains("error"));
}

// ===========================================================================
// 29. Existing Phase 5 search behavior remains unchanged after ingestion
// ===========================================================================

TEST_F(HttpApiTest, SearchBehaviorUnchangedAfterIngestion)
{
    start_server();

    // Ingest a document
    post("/documents",
         R"({"id": 800, "content": "new searchable document"})");

    // Verify original index still works correctly
    const auto [s1, b1] = get("/search?q=fox");
    EXPECT_EQ(s1, 200);
    const auto j1 = nlohmann::json::parse(b1);
    EXPECT_EQ(j1["total"], 2u);  // original docs 1 and 4

    // AND mode still works
    const auto [s2, b2] = get("/search?q=quick+fox&mode=and");
    EXPECT_EQ(s2, 200);
    const auto j2 = nlohmann::json::parse(b2);
    EXPECT_EQ(j2["total"], 1u);  // only doc 1
}

// ===========================================================================
// 30. Response content type is application/json for ingestion
// ===========================================================================

TEST_F(HttpApiTest, IngestionResponseContentTypeIsJson)
{
    start_server();
    httplib::Client client("localhost", server_->port());

    auto res = client.Post("/documents",
                           R"({"id": 900, "content": "test"})",
                           "application/json");
    ASSERT_TRUE(res);
    EXPECT_EQ(res->status, 201);
    EXPECT_NE(res->get_header_value("Content-Type").find("application/json"),
              std::string::npos);
}

// ===========================================================================
// Phase 9: PUT /documents/{id} — Update Tests
// ===========================================================================

TEST_F(HttpApiTest, PutUpdateSuccessReturns200)
{
    start_server();

    // Create a document first
    post("/documents", R"({"id": 1000, "content": "original alpha"})");

    // Update it
    const auto [status, body] = put(
        "/documents/1000",
        R"({"content": "updated beta"})");
    EXPECT_EQ(status, 200);

    const auto j = nlohmann::json::parse(body);
    EXPECT_EQ(j["document_id"], 1000u);
    EXPECT_GE(j["terms_indexed"].get<std::size_t>(), 1u);
}

TEST_F(HttpApiTest, PutUpdateMissingDocumentReturns404)
{
    start_server();
    const auto [status, body] = put(
        "/documents/9999",
        R"({"content": "new content"})");
    EXPECT_EQ(status, 404);
    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.contains("error"));
}

TEST_F(HttpApiTest, PutUpdateThenSearchFindsNewContent)
{
    start_server();

    // Ingest with old content
    post("/documents", R"({"id": 1001, "content": "alpha beta"})");

    // Search for old content
    auto [s1, b1] = get("/search?q=alpha");
    auto j1 = nlohmann::json::parse(b1);
    EXPECT_EQ(j1["total"], 1u);
    EXPECT_EQ(j1["results"][0]["document_id"], 1001u);

    // Update with new content
    put("/documents/1001", R"({"content": "gamma delta"})");

    // Old content no longer found
    auto [s2, b2] = get("/search?q=alpha");
    auto j2 = nlohmann::json::parse(b2);
    EXPECT_EQ(j2["total"], 0u);

    // New content found
    auto [s3, b3] = get("/search?q=gamma");
    auto j3 = nlohmann::json::parse(b3);
    EXPECT_EQ(j3["total"], 1u);
    EXPECT_EQ(j3["results"][0]["document_id"], 1001u);
}

TEST_F(HttpApiTest, PutInvalidContentReturns400)
{
    start_server();
    post("/documents", R"({"id": 1002, "content": "original"})");

    // Empty content
    const auto [s1, b1] = put("/documents/1002", R"({"content": ""})");
    EXPECT_EQ(s1, 400);
}

TEST_F(HttpApiTest, PutMalformedJsonReturns400)
{
    start_server();
    post("/documents", R"({"id": 1003, "content": "original"})");

    const auto [status, body] = put("/documents/1003", "not json");
    EXPECT_EQ(status, 400);
}

TEST_F(HttpApiTest, PutMissingContentFieldReturns400)
{
    start_server();
    post("/documents", R"({"id": 1004, "content": "original"})");

    const auto [status, body] = put("/documents/1004", R"({})");
    EXPECT_EQ(status, 400);
}

// ===========================================================================
// Phase 9: DELETE /documents/{id} — Delete Tests
// ===========================================================================

TEST_F(HttpApiTest, DeleteSuccessReturns204)
{
    start_server();
    post("/documents", R"({"id": 2000, "content": "to be deleted"})");

    const auto [status, body] = del("/documents/2000");
    EXPECT_EQ(status, 204);
    EXPECT_TRUE(body.empty());
}

TEST_F(HttpApiTest, DeleteMissingDocumentReturns404)
{
    start_server();
    const auto [status, body] = del("/documents/9999");
    EXPECT_EQ(status, 404);
    const auto j = nlohmann::json::parse(body);
    EXPECT_TRUE(j.contains("error"));
}

TEST_F(HttpApiTest, DeleteThenSearchFindsNothing)
{
    start_server();
    post("/documents",
         R"({"id": 2001, "content": "unique_delete_test"})");

    // Verify it's searchable
    auto [s1, b1] = get("/search?q=unique_delete_test");
    auto j1 = nlohmann::json::parse(b1);
    EXPECT_EQ(j1["total"], 1u);

    // Delete it
    del("/documents/2001");

    // No longer found
    auto [s2, b2] = get("/search?q=unique_delete_test");
    auto j2 = nlohmann::json::parse(b2);
    EXPECT_EQ(j2["total"], 0u);
}

TEST_F(HttpApiTest, DeletePreservesOtherDocuments)
{
    start_server();
    // Use terms that are unique to each document and don't share
    // tokens. The tokenizer splits on non-alphanumeric characters,
    // so "alpha beta" and "gamma delta" share no tokens.
    post("/documents",
         R"({"id": 2002, "content": "alpha beta"})");
    post("/documents",
         R"({"id": 2003, "content": "gamma delta"})");

    // Delete doc 2002 — verify it returns 204
    const auto [del_status, del_body] = del("/documents/2002");
    EXPECT_EQ(del_status, 204);

    // Doc 2003 still searchable
    auto [s, b] = get("/search?q=gamma+delta");
    auto j = nlohmann::json::parse(b);
    EXPECT_EQ(j["total"], 1u);
    EXPECT_EQ(j["results"][0]["document_id"], 2003u);

    // Doc 2002 no longer searchable — "alpha" only existed in doc 2002
    auto [s2, b2] = get("/search?q=alpha");
    auto j2 = nlohmann::json::parse(b2);
    EXPECT_EQ(j2["total"], 0u);
}

TEST_F(HttpApiTest, DeleteExistingThenReIngest)
{
    start_server();
    post("/documents",
         R"({"id": 2004, "content": "original content"})");

    del("/documents/2004");

    // Re-create with different content
    const auto [s, b] = post(
        "/documents",
        R"({"id": 2004, "content": "new content"})");
    EXPECT_EQ(s, 201);

    // New content is searchable
    auto [s2, b2] = get("/search?q=new+content");
    auto j = nlohmann::json::parse(b2);
    EXPECT_EQ(j["total"], 1u);
    EXPECT_EQ(j["results"][0]["document_id"], 2004u);
}

} // namespace
