// Distributed Search Engine - Search Service tests (Phase 5B-1).
//
// Unit tests for dse::SearchService. These tests exercise the business
// logic without any HTTP transport. Tests verify:
// - Basic AND/OR search
// - Result limiting
// - Request validation
// - Ranking order
// - Empty index behavior
// - Edge cases
//
// HTTP integration tests (real requests to localhost) will be added
// in Phase 5B-2.

#include <gtest/gtest.h>

#include "inverted_index.h"
#include "ranker.h"
#include "search_service.h"
#include "tokenizer.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace {

using dse::InvertedIndex;
using dse::SearchMode;
using dse::SearchRequest;
using dse::SearchResponse;
using dse::SearchService;
using dse::doc_id;

// Build an InvertedIndex from a list of (id, text) pairs for reuse.
InvertedIndex build_index(
    std::initializer_list<std::pair<doc_id, std::string_view>> docs)
{
    InvertedIndex index;
    for (const auto& [id, text] : docs) {
        index.add_document(id, text);
    }
    return index;
}

// Helper: extract document IDs from a SearchResponse.
std::vector<doc_id> ids(const SearchResponse& resp)
{
    std::vector<doc_id> result;
    result.reserve(resp.results.size());
    for (const auto& r : resp.results) {
        result.push_back(r.document_id);
    }
    return result;
}

} // namespace

// ===========================================================================
// 1. Basic search
// ===========================================================================

TEST(SearchServiceBasic, ORQueryReturnsMatchingDocuments)
{
    const auto index = build_index({
        {1, "cat"},
        {2, "dog"},
        {3, "cat dog"},
    });
    const SearchService svc(index);

    SearchRequest req;
    req.query = "cat";
    req.mode = SearchMode::Or;
    const auto resp = svc.search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 2u);  // docs 1 and 3
    EXPECT_EQ(ids(resp), (std::vector<doc_id>{1, 3}));
}

TEST(SearchServiceBasic, ANDQueryReturnsIntersection)
{
    const auto index = build_index({
        {1, "cat"},
        {2, "dog"},
        {3, "cat dog"},
    });
    const SearchService svc(index);

    SearchRequest req;
    req.query = "cat dog";
    req.mode = SearchMode::And;
    const auto resp = svc.search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 1u);  // only doc 3
    EXPECT_EQ(ids(resp), (std::vector<doc_id>{3}));
}

TEST(SearchServiceBasic, MultiTermORUnion)
{
    const auto index = build_index({
        {1, "alpha"},
        {2, "beta"},
        {3, "gamma"},
    });
    const SearchService svc(index);

    SearchRequest req;
    req.query = "alpha beta gamma";
    req.mode = SearchMode::Or;
    const auto resp = svc.search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 3u);
}

TEST(SearchServiceBasic, MultiTermANDAllPresent)
{
    const auto index = build_index({
        {1, "alpha beta"},
        {2, "alpha"},
        {3, "beta"},
    });
    const SearchService svc(index);

    SearchRequest req;
    req.query = "alpha beta";
    req.mode = SearchMode::And;
    const auto resp = svc.search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 1u);
    EXPECT_EQ(ids(resp), (std::vector<doc_id>{1}));
}

// ===========================================================================
// 2. Result limiting
// ===========================================================================

TEST(SearchLimit, DefaultLimitIsTen)
{
    InvertedIndex index;
    for (int i = 1; i <= 20; ++i) {
        index.add_document(static_cast<doc_id>(i), "common");
    }
    const SearchService svc(index);

    SearchRequest req;
    req.query = "common";
    const auto resp = svc.search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 20u);
    EXPECT_EQ(resp.results.size(), 10u);  // default limit
}

TEST(SearchLimit, LimitOneReturnsSingleResult)
{
    const auto index = build_index({
        {1, "cat"},
        {2, "cat"},
        {3, "cat"},
    });
    const SearchService svc(index);

    SearchRequest req;
    req.query = "cat";
    req.limit = 1;
    const auto resp = svc.search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 3u);
    EXPECT_EQ(resp.results.size(), 1u);
}

TEST(SearchLimit, LimitExceedsResultsReturnsAll)
{
    const auto index = build_index({
        {1, "cat"},
        {2, "dog"},
    });
    const SearchService svc(index);

    SearchRequest req;
    req.query = "cat dog";
    req.mode = SearchMode::Or;
    req.limit = 100;
    const auto resp = svc.search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 2u);
    EXPECT_EQ(resp.results.size(), 2u);  // only 2 exist
}

TEST(SearchLimit, LimitFiveOnTenResults)
{
    InvertedIndex index;
    for (int i = 1; i <= 10; ++i) {
        index.add_document(static_cast<doc_id>(i), "common");
    }
    const SearchService svc(index);

    SearchRequest req;
    req.query = "common";
    req.limit = 5;
    const auto resp = svc.search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 10u);
    EXPECT_EQ(resp.results.size(), 5u);
}

// ===========================================================================
// 3. Request validation
// ===========================================================================

TEST(SearchValidation, EmptyQueryReturnsError)
{
    const auto index = build_index({{1, "cat"}});
    const SearchService svc(index);

    SearchRequest req;
    req.query = "";
    const auto resp = svc.search(req);

    EXPECT_TRUE(resp.is_error);
}

TEST(SearchValidation, WhitespaceOnlyQueryReturnsError)
{
    const auto index = build_index({{1, "cat"}});
    const SearchService svc(index);

    SearchRequest req;
    req.query = "   ";
    const auto resp = svc.search(req);

    EXPECT_TRUE(resp.is_error);
}

TEST(SearchValidation, ZeroLimitReturnsError)
{
    const auto index = build_index({{1, "cat"}});
    const SearchService svc(index);

    SearchRequest req;
    req.query = "cat";
    req.limit = 0;
    const auto resp = svc.search(req);

    EXPECT_TRUE(resp.is_error);
}

TEST(SearchValidation, ValidRequestPasses)
{
    const auto index = build_index({{1, "cat"}});
    const SearchService svc(index);

    SearchRequest req;
    req.query = "cat";
    EXPECT_TRUE(SearchService::validate_request(req));
}

TEST(SearchValidation, ValidateEmptyQuery)
{
    SearchRequest req;
    req.query = "";
    EXPECT_FALSE(SearchService::validate_request(req));
}

TEST(SearchValidation, ValidateWhitespaceQuery)
{
    SearchRequest req;
    req.query = "\t\n";
    EXPECT_FALSE(SearchService::validate_request(req));
}

TEST(SearchValidation, ValidateZeroLimit)
{
    SearchRequest req;
    req.query = "cat";
    req.limit = 0;
    EXPECT_FALSE(SearchService::validate_request(req));
}

// ===========================================================================
// 4. Ranking order
// ===========================================================================

TEST(SearchRanking, ResultsSortedByScoreDesc)
{
    const auto index = build_index({
        {1, "cat"},
        {2, "cat cat cat cat cat"},
        {3, "cat cat"},
        {4, "dog"},
    });
    const SearchService svc(index);

    SearchRequest req;
    req.query = "cat";
    req.mode = SearchMode::Or;
    const auto resp = svc.search(req);

    EXPECT_FALSE(resp.is_error);
    ASSERT_EQ(resp.results.size(), 3u);
    // Doc 2 (tf=5) > Doc 3 (tf=2) > Doc 1 (tf=1)
    EXPECT_EQ(resp.results[0].document_id, 2u);
    EXPECT_EQ(resp.results[1].document_id, 3u);
    EXPECT_EQ(resp.results[2].document_id, 1u);

    // Verify scores are descending
    for (std::size_t i = 1; i < resp.results.size(); ++i) {
        EXPECT_GE(resp.results[i - 1].score, resp.results[i].score);
    }
}

TEST(SearchRanking, LimitedResultsStillTopScored)
{
    const auto index = build_index({
        {1, "cat"},
        {2, "cat cat cat cat cat"},
        {3, "cat cat"},
        {4, "cat cat cat"},
        {5, "dog"},
    });
    const SearchService svc(index);

    SearchRequest req;
    req.query = "cat";
    req.limit = 2;
    const auto resp = svc.search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 4u);  // 4 docs have "cat"
    EXPECT_EQ(resp.results.size(), 2u);
    // Top 2 by TF: doc 2 (tf=5) and doc 4 (tf=4) or doc 3 (tf=2)
    EXPECT_EQ(resp.results[0].document_id, 2u);  // highest TF
}

// ===========================================================================
// 5. Empty index
// ===========================================================================

TEST(SearchEmptyIndex, SearchOnEmptyIndex)
{
    InvertedIndex index;
    const SearchService svc(index);

    SearchRequest req;
    req.query = "anything";
    const auto resp = svc.search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 0u);
    EXPECT_TRUE(resp.results.empty());
}

TEST(SearchEmptyIndex, NoMatchingTerms)
{
    const auto index = build_index({{1, "cat"}, {2, "dog"}});
    const SearchService svc(index);

    SearchRequest req;
    req.query = "nonexistent";
    const auto resp = svc.search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 0u);
    EXPECT_TRUE(resp.results.empty());
}

// ===========================================================================
// 6. Edge cases
// ===========================================================================

TEST(SearchEdgeCases, SingleDocument)
{
    const auto index = build_index({{1, "hello world"}});
    const SearchService svc(index);

    SearchRequest req;
    req.query = "hello";
    const auto resp = svc.search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 1u);
    EXPECT_EQ(resp.results[0].document_id, 1u);
}

TEST(SearchEdgeCases, QueryWithPunctuation)
{
    const auto index = build_index({
        {1, "search engine"},
        {2, "search-engine"},
    });
    const SearchService svc(index);

    SearchRequest req;
    req.query = "Search, Engine!";  // tokenizes to ["search", "engine"]
    req.mode = SearchMode::And;
    const auto resp = svc.search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 2u);  // both docs have "search" and "engine"
}

TEST(SearchEdgeCases, CaseInsensitiveQuery)
{
    const auto index = build_index({{1, "cat"}, {2, "CAT"}, {3, "cAt"}});
    const SearchService svc(index);

    SearchRequest req;
    req.query = "CAT";
    const auto resp = svc.search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 3u);  // all docs normalized to "cat"
}

TEST(SearchEdgeCases, ResponseFieldsPopulated)
{
    const auto index = build_index({{1, "cat"}, {2, "dog"}});
    const SearchService svc(index);

    SearchRequest req;
    req.query = "cat dog";
    req.mode = SearchMode::And;
    req.limit = 5;
    const auto resp = svc.search(req);

    EXPECT_EQ(resp.query, "cat dog");
    EXPECT_EQ(resp.mode, "and");
    EXPECT_EQ(resp.limit, 5u);
}

TEST(SearchEdgeCases, ResponseFieldsForOR)
{
    const auto index = build_index({{1, "cat"}, {2, "dog"}});
    const SearchService svc(index);

    SearchRequest req;
    req.query = "cat";
    req.mode = SearchMode::Or;
    const auto resp = svc.search(req);

    EXPECT_EQ(resp.mode, "or");
}

// ===========================================================================
// 7. Determinism
// ===========================================================================

TEST(SearchDeterminism, SameQuerySameResult)
{
    const auto index = build_index({
        {1, "cat dog"},
        {2, "cat"},
        {3, "dog"},
    });
    const SearchService svc(index);

    SearchRequest req;
    req.query = "cat dog";
    req.mode = SearchMode::Or;

    const auto r1 = svc.search(req);
    const auto r2 = svc.search(req);
    EXPECT_EQ(r1.results, r2.results);
}

TEST(SearchDeterminism, DifferentInsertionOrderSameResult)
{
    auto a = build_index({{1, "cat"}, {2, "cat dog"}, {3, "dog"}});
    auto b = build_index({{3, "dog"}, {1, "cat"}, {2, "cat dog"}});

    const SearchService sa(a);
    const SearchService sb(b);

    SearchRequest req;
    req.query = "cat dog";
    req.mode = SearchMode::Or;

    EXPECT_EQ(sa.search(req).results, sb.search(req).results);
}

// ===========================================================================
// 8. Integration with ranker
// ===========================================================================

TEST(SearchIntegration, ScoresAreTFIDF)
{
    const auto index = build_index({
        {1, "cat"},
        {2, "dog"},
        {3, "cat dog"},
    });
    const SearchService svc(index);

    SearchRequest req;
    req.query = "cat dog";
    req.mode = SearchMode::Or;
    const auto resp = svc.search(req);

    EXPECT_FALSE(resp.is_error);
    ASSERT_EQ(resp.total, 3u);

    // Doc 3 matches both terms → highest score
    EXPECT_EQ(resp.results[0].document_id, 3u);
    EXPECT_GT(resp.results[0].score, resp.results[1].score);
}

TEST(SearchIntegration, ANDMissesPartialMatches)
{
    const auto index = build_index({
        {1, "the cat sat"},
        {2, "the dog ran"},
        {3, "the cat and dog played"},
    });
    const SearchService svc(index);

    SearchRequest req;
    req.query = "cat dog";
    req.mode = SearchMode::And;
    const auto resp = svc.search(req);

    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.total, 1u);
    EXPECT_EQ(resp.results[0].document_id, 3u);
}
