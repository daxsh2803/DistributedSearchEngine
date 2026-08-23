// Distributed Search Engine - Node Wire Format Tests (Phase 12A).
//
// Verifies JSON round-trip serialization for all NodeClient request
// and response types. Each test serializes a value to JSON and
// deserializes it back, checking that the result matches the original.

#include <gtest/gtest.h>

#include <cstdint>
#include <string>
#include <vector>

#include "node_client.h"
#include "node_wire.h"

using namespace dse;

// ===========================================================================
// Search request round-trip
// ===========================================================================

TEST(NodeWireTest, SearchRequestRoundTrip)
{
    ShardSearchRequest req{3, {"quick", "fox", "brown"}};
    auto j = to_json(req);
    auto result = shard_search_request_from_json(j);

    EXPECT_EQ(result.shard_id, 3u);
    ASSERT_EQ(result.terms.size(), 3u);
    EXPECT_EQ(result.terms[0], "quick");
    EXPECT_EQ(result.terms[1], "fox");
    EXPECT_EQ(result.terms[2], "brown");
}

TEST(NodeWireTest, SearchRequestEmptyTerms)
{
    ShardSearchRequest req{0, {}};
    auto j = to_json(req);
    auto result = shard_search_request_from_json(j);

    EXPECT_EQ(result.shard_id, 0u);
    EXPECT_TRUE(result.terms.empty());
}

TEST(NodeWireTest, SearchRequestSingleTerm)
{
    ShardSearchRequest req{7, {"distributed"}};
    auto j = to_json(req);
    auto result = shard_search_request_from_json(j);

    EXPECT_EQ(result.shard_id, 7u);
    ASSERT_EQ(result.terms.size(), 1u);
    EXPECT_EQ(result.terms[0], "distributed");
}

// ===========================================================================
// Search response round-trip
// ===========================================================================

TEST(NodeWireTest, SearchResponseRoundTrip)
{
    ShardSearchResponse resp;
    resp.shard_id = 2;
    resp.local_document_count = 42;
    resp.is_error = false;
    resp.error_message = "";
    resp.terms_postings = {
        {{1, 3}, {5, 1}},       // term 0
        {{2, 2}},                // term 1
        {}                       // term 2 (no matches)
    };

    auto j = to_json(resp);
    auto result = shard_search_response_from_json(j);

    EXPECT_EQ(result.shard_id, 2u);
    EXPECT_EQ(result.local_document_count, 42u);
    EXPECT_FALSE(result.is_error);
    EXPECT_TRUE(result.error_message.empty());

    ASSERT_EQ(result.terms_postings.size(), 3u);
    ASSERT_EQ(result.terms_postings[0].size(), 2u);
    EXPECT_EQ(result.terms_postings[0][0].document_id, 1u);
    EXPECT_EQ(result.terms_postings[0][0].term_frequency, 3u);
    EXPECT_EQ(result.terms_postings[0][1].document_id, 5u);
    EXPECT_EQ(result.terms_postings[0][1].term_frequency, 1u);

    ASSERT_EQ(result.terms_postings[1].size(), 1u);
    EXPECT_EQ(result.terms_postings[1][0].document_id, 2u);
    EXPECT_EQ(result.terms_postings[1][0].term_frequency, 2u);

    EXPECT_TRUE(result.terms_postings[2].empty());
}

TEST(NodeWireTest, SearchResponseError)
{
    ShardSearchResponse resp;
    resp.shard_id = 0;
    resp.local_document_count = 0;
    resp.is_error = true;
    resp.error_message = "Shard 0 not found on node 1";

    auto j = to_json(resp);
    auto result = shard_search_response_from_json(j);

    EXPECT_TRUE(result.is_error);
    EXPECT_EQ(result.error_message, "Shard 0 not found on node 1");
    EXPECT_EQ(result.local_document_count, 0u);
    EXPECT_TRUE(result.terms_postings.empty());
}

TEST(NodeWireTest, SearchResponseEmptyPostings)
{
    ShardSearchResponse resp;
    resp.shard_id = 0;
    resp.local_document_count = 10;
    resp.terms_postings = {};

    auto j = to_json(resp);
    auto result = shard_search_response_from_json(j);

    EXPECT_EQ(result.local_document_count, 10u);
    EXPECT_TRUE(result.terms_postings.empty());
}

// ===========================================================================
// Write request round-trip (add/update)
// ===========================================================================

TEST(NodeWireTest, WriteRequestRoundTrip)
{
    ShardWriteRequest req{1, 42, "hello world"};
    auto j = to_json(req);
    auto result = shard_write_request_from_json(j);

    EXPECT_EQ(result.shard_id, 1u);
    EXPECT_EQ(result.document_id, 42u);
    EXPECT_EQ(result.content, "hello world");
}

TEST(NodeWireTest, WriteRequestEmptyContent)
{
    ShardWriteRequest req{0, 0, ""};
    auto j = to_json(req);
    auto result = shard_write_request_from_json(j);

    EXPECT_EQ(result.shard_id, 0u);
    EXPECT_EQ(result.document_id, 0u);
    EXPECT_EQ(result.content, "");
}

TEST(NodeWireTest, WriteRequestLargeContent)
{
    std::string big_content(10000, 'x');
    ShardWriteRequest req{5, 999999, big_content};
    auto j = to_json(req);
    auto result = shard_write_request_from_json(j);

    EXPECT_EQ(result.shard_id, 5u);
    EXPECT_EQ(result.document_id, 999999u);
    EXPECT_EQ(result.content, big_content);
}

// ===========================================================================
// Write response round-trip
// ===========================================================================

TEST(NodeWireTest, WriteResponseRoundTrip)
{
    ShardWriteResponse resp;
    resp.shard_id = 1;
    resp.document_id = 42;
    resp.terms_indexed = 5;
    resp.is_error = false;
    resp.error_message = "";

    auto j = to_json(resp);
    auto result = shard_write_response_from_json(j);

    EXPECT_EQ(result.shard_id, 1u);
    EXPECT_EQ(result.document_id, 42u);
    EXPECT_EQ(result.terms_indexed, 5u);
    EXPECT_FALSE(result.is_error);
}

TEST(NodeWireTest, WriteResponseError)
{
    ShardWriteResponse resp;
    resp.shard_id = 0;
    resp.document_id = 42;
    resp.terms_indexed = 0;
    resp.is_error = true;
    resp.error_message = "Document with id 42 already exists";

    auto j = to_json(resp);
    auto result = shard_write_response_from_json(j);

    EXPECT_TRUE(result.is_error);
    EXPECT_EQ(result.error_message, "Document with id 42 already exists");
}

// ===========================================================================
// Remove request round-trip
// ===========================================================================

TEST(NodeWireTest, RemoveRequestRoundTrip)
{
    ShardRemoveRequest req{2, 100};
    auto j = to_json(req);
    auto result = shard_remove_request_from_json(j);

    EXPECT_EQ(result.shard_id, 2u);
    EXPECT_EQ(result.document_id, 100u);
}

TEST(NodeWireTest, RemoveRequestBoundaryIds)
{
    ShardRemoveRequest req{0, 0};
    auto j = to_json(req);
    auto result = shard_remove_request_from_json(j);

    EXPECT_EQ(result.shard_id, 0u);
    EXPECT_EQ(result.document_id, 0u);
}

// ===========================================================================
// Remove response round-trip
// ===========================================================================

TEST(NodeWireTest, RemoveResponseRoundTrip)
{
    ShardRemoveResponse resp;
    resp.shard_id = 2;
    resp.is_error = false;
    resp.error_message = "";

    auto j = to_json(resp);
    auto result = shard_remove_response_from_json(j);

    EXPECT_EQ(result.shard_id, 2u);
    EXPECT_FALSE(result.is_error);
}

TEST(NodeWireTest, RemoveResponseError)
{
    ShardRemoveResponse resp;
    resp.shard_id = 1;
    resp.is_error = true;
    resp.error_message = "Document with id 50 not found";

    auto j = to_json(resp);
    auto result = shard_remove_response_from_json(j);

    EXPECT_TRUE(result.is_error);
    EXPECT_EQ(result.error_message, "Document with id 50 not found");
}

// ===========================================================================
// Get request round-trip
// ===========================================================================

TEST(NodeWireTest, GetRequestRoundTrip)
{
    ShardGetRequest req{3, 77};
    auto j = to_json(req);
    auto result = shard_get_request_from_json(j);

    EXPECT_EQ(result.shard_id, 3u);
    EXPECT_EQ(result.document_id, 77u);
}

// ===========================================================================
// Get response round-trip
// ===========================================================================

TEST(NodeWireTest, GetResponseFound)
{
    ShardGetResponse resp;
    resp.shard_id = 0;
    resp.document_id = 10;
    resp.content = "the quick brown fox";
    resp.found = true;

    auto j = to_json(resp);
    auto result = shard_get_response_from_json(j);

    EXPECT_EQ(result.shard_id, 0u);
    EXPECT_EQ(result.document_id, 10u);
    EXPECT_EQ(result.content, "the quick brown fox");
    EXPECT_TRUE(result.found);
}

TEST(NodeWireTest, GetResponseNotFound)
{
    ShardGetResponse resp;
    resp.shard_id = 1;
    resp.document_id = 999;
    resp.content = "";
    resp.found = false;

    auto j = to_json(resp);
    auto result = shard_get_response_from_json(j);

    EXPECT_FALSE(result.found);
    EXPECT_TRUE(result.content.empty());
}

// ===========================================================================
// Count request round-trip
// ===========================================================================

TEST(NodeWireTest, CountRequestRoundTrip)
{
    ShardCountRequest req{4};
    auto j = to_json(req);
    auto result = shard_count_request_from_json(j);

    EXPECT_EQ(result.shard_id, 4u);
}

// ===========================================================================
// Count response round-trip
// ===========================================================================

TEST(NodeWireTest, CountResponseRoundTrip)
{
    ShardCountResponse resp;
    resp.shard_id = 0;
    resp.document_count = 12345;

    auto j = to_json(resp);
    auto result = shard_count_response_from_json(j);

    EXPECT_EQ(result.shard_id, 0u);
    EXPECT_EQ(result.document_count, 12345u);
}

TEST(NodeWireTest, CountResponseZero)
{
    ShardCountResponse resp;
    resp.shard_id = 7;
    resp.document_count = 0;

    auto j = to_json(resp);
    auto result = shard_count_response_from_json(j);

    EXPECT_EQ(result.document_count, 0u);
}

// ===========================================================================
// Persistence request/response round-trip
// ===========================================================================

TEST(NodeWireTest, PersistenceRequestRoundTrip)
{
    auto j = shard_persistence_request_to_json(42);
    auto result = shard_persistence_request_from_json(j);

    EXPECT_EQ(result, 42u);
}

TEST(NodeWireTest, PersistenceRequestZero)
{
    auto j = shard_persistence_request_to_json(0);
    auto result = shard_persistence_request_from_json(j);

    EXPECT_EQ(result, 0u);
}

TEST(NodeWireTest, PersistenceResponseSuccess)
{
    auto j = shard_persistence_response_to_json(true);
    EXPECT_TRUE(shard_persistence_response_from_json(j));
}

TEST(NodeWireTest, PersistenceResponseFailure)
{
    auto j = shard_persistence_response_to_json(false);
    EXPECT_FALSE(shard_persistence_response_from_json(j));
}

// ===========================================================================
// JSON string serialization
// ===========================================================================

TEST(NodeWireTest, SearchRequestJsonString)
{
    ShardSearchRequest req{0, {"test"}};
    auto j = to_json(req);
    std::string s = j.dump();

    EXPECT_NE(s.find("\"shard_id\""), std::string::npos);
    EXPECT_NE(s.find("\"terms\""), std::string::npos);
    EXPECT_NE(s.find("\"test\""), std::string::npos);
}

TEST(NodeWireTest, WriteResponseJsonString)
{
    ShardWriteResponse resp;
    resp.shard_id = 1;
    resp.document_id = 5;
    resp.terms_indexed = 3;
    resp.is_error = false;
    auto j = to_json(resp);
    std::string s = j.dump();

    EXPECT_NE(s.find("\"shard_id\""), std::string::npos);
    EXPECT_NE(s.find("\"document_id\""), std::string::npos);
    EXPECT_NE(s.find("\"terms_indexed\""), std::string::npos);
    EXPECT_NE(s.find("\"is_error\""), std::string::npos);
}
