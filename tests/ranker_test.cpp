// Distributed Search Engine - Ranker tests (Phase 4B).
//
// Real coverage for dse::Ranker per the Phase 4A testing strategy
// (docs/learning/phase-4-ranking.md, Section 14). Tests verify:
// - IDF computation matches hand-calculated values
// - TF-IDF scoring correctness
// - Ranking order (score descending)
// - AND vs OR semantics
// - Missing terms, empty queries, edge cases
// - Determinism and tie-breaking
// - End-to-end integration with tokenizer and index

#include <gtest/gtest.h>

#include "inverted_index.h"
#include "ranker.h"
#include "tokenizer.h"

#include <cmath>
#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace {

using dse::InvertedIndex;
using dse::Posting;
using dse::RankedResult;
using dse::Ranker;
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


} // namespace

// ===========================================================================
// 1. IDF computation
// ===========================================================================

TEST(RankerIDF, SingleTermSingleDoc)
{
    // N=1, df=1: idf = ln(1/1) = 0
    const auto index = build_index({{1, "hello"}});
    const Ranker ranker(index);

    const auto results = ranker.ranked_or("hello");
    ASSERT_EQ(results.size(), 1u);
    EXPECT_DOUBLE_EQ(results[0].score, 0.0);  // tf=1 × idf=0 = 0
}

TEST(RankerIDF, TwoDocumentsOneTerm)
{
    // N=2, df=1 for "cat": idf = ln(2/1) ≈ 0.693147
    const auto index = build_index({{1, "cat"}, {2, "dog"}});
    const Ranker ranker(index);

    const auto results = ranker.ranked_or("cat");
    ASSERT_EQ(results.size(), 1u);
    EXPECT_DOUBLE_EQ(results[0].score, std::log(2.0));
}

TEST(RankerIDF, CommonTerm)
{
    // N=10, "the" in all 10: idf = ln(10/10) = 0
    InvertedIndex index;
    for (int i = 1; i <= 10; ++i) {
        index.add_document(static_cast<doc_id>(i), "the cat");
    }
    const Ranker ranker(index);

    const auto results = ranker.ranked_or("the");
    // "the" has idf=0, so all scores should be 0
    for (const auto& r : results) {
        EXPECT_DOUBLE_EQ(r.score, 0.0);
    }
}

TEST(RankerIDF, RareTerm)
{
    // N=100, df=1: idf = ln(100) ≈ 4.605
    InvertedIndex index;
    for (int i = 1; i <= 99; ++i) {
        index.add_document(static_cast<doc_id>(i), "common");
    }
    index.add_document(100, "axolotl");
    const Ranker ranker(index);

    const auto results = ranker.ranked_or("axolotl");
    ASSERT_EQ(results.size(), 1u);
    EXPECT_DOUBLE_EQ(results[0].score, std::log(100.0));
}

// ===========================================================================
// 2. Basic TF-IDF scoring
// ===========================================================================

TEST(RankerScore, SingleTermSingleDocument)
{
    // N=1, doc has "cat" twice: tf=2, idf=0 → score = 0
    const auto index = build_index({{1, "cat cat"}});
    const Ranker ranker(index);

    const auto results = ranker.ranked_or("cat");
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].document_id, 1u);
    EXPECT_DOUBLE_EQ(results[0].score, 0.0);
}

TEST(RankerScore, TwoDocumentsDifferentTF)
{
    // N=2, both docs have "cat", doc 1 has tf=1, doc 2 has tf=3
    // idf = ln(2/2) = 0 → all scores 0
    const auto index = build_index({{1, "cat"}, {2, "cat cat cat"}});
    const Ranker ranker(index);

    const auto results = ranker.ranked_or("cat");
    ASSERT_EQ(results.size(), 2u);
    // Both scores are 0 (idf=0 when df=N)
    EXPECT_DOUBLE_EQ(results[0].score, 0.0);
    EXPECT_DOUBLE_EQ(results[1].score, 0.0);
}

TEST(RankerScore, TwoDocumentsOneRare)
{
    // N=2, doc 1 has "cat" (df=1, idf=ln(2)), doc 2 has "dog" (df=1, idf=ln(2))
    const auto index = build_index({{1, "cat"}, {2, "dog"}});
    const Ranker ranker(index);

    const auto results = ranker.ranked_or("cat dog");
    ASSERT_EQ(results.size(), 2u);
    EXPECT_DOUBLE_EQ(results[0].score, std::log(2.0));
    EXPECT_DOUBLE_EQ(results[1].score, std::log(2.0));
}

TEST(RankerScore, HigherTFMeansHigherScore)
{
    // N=3, all docs have "cat", doc 1 has tf=1, doc 2 has tf=5
    // idf = ln(3/3) = 0 → all scores 0
    // But if we add a doc without "cat" to make df < N:
    // N=4, docs 1,2,3 have "cat" (df=3), doc 4 doesn't
    // idf = ln(4/3) ≈ 0.2877
    const auto index = build_index({
        {1, "cat"},
        {2, "cat cat cat cat cat"},
        {3, "cat cat"},
        {4, "dog"},
    });
    const Ranker ranker(index);

    const auto results = ranker.ranked_or("cat");
    ASSERT_EQ(results.size(), 3u);
    // Doc 2 (tf=5) > Doc 3 (tf=2) > Doc 1 (tf=1)
    EXPECT_EQ(results[0].document_id, 2u);
    EXPECT_EQ(results[1].document_id, 3u);
    EXPECT_EQ(results[2].document_id, 1u);
}

// ===========================================================================
// 3. Multi-term scoring
// ===========================================================================

TEST(RankerMultiTerm, DocumentMatchesBothTerms)
{
    // N=3, query "cat dog"
    // "cat": df=2 (docs 1,3), idf=ln(3/2)
    // "dog": df=2 (docs 2,3), idf=ln(3/2)
    const auto index = build_index({
        {1, "cat"},
        {2, "dog"},
        {3, "cat dog"},
    });
    const Ranker ranker(index);

    const auto results = ranker.ranked_or("cat dog");
    ASSERT_EQ(results.size(), 3u);
    // Doc 3 has both: score = 1×ln(3/2) + 1×ln(3/2) = 2×ln(3/2)
    // Doc 1: score = 1×ln(3/2) = ln(3/2)
    // Doc 2: score = 1×ln(3/2) = ln(3/2)
    EXPECT_GT(results[0].score, results[1].score);
    EXPECT_DOUBLE_EQ(results[0].score, 2.0 * std::log(3.0 / 2.0));
    EXPECT_EQ(results[0].document_id, 3u);
}

TEST(RankerMultiTerm, ScoresSumAcrossTerms)
{
    // N=2, doc 1 has "cat dog", doc 2 has only "cat"
    // "cat": df=2, idf=ln(2/2)=0 → no contribution
    // Need N > df to get nonzero IDF
    const auto index = build_index({
        {1, "cat dog"},
        {2, "cat"},
        {3, "dog"},
    });
    const Ranker ranker(index);

    const auto results = ranker.ranked_or("cat dog");
    ASSERT_EQ(results.size(), 3u);
    // "cat": df=2, idf=ln(3/2); "dog": df=2, idf=ln(3/2)
    // Doc 1: 1×ln(3/2) + 1×ln(3/2) = 2×ln(3/2)
    // Doc 2: 1×ln(3/2) = ln(3/2)
    // Doc 3: 1×ln(3/2) = ln(3/2)
    EXPECT_EQ(results[0].document_id, 1u);
    EXPECT_DOUBLE_EQ(results[0].score, 2.0 * std::log(3.0 / 2.0));
}

// ===========================================================================
// 4. Ranking order
// ===========================================================================

TEST(RankerOrder, ScoreDescending)
{
    // N=5, various TF values
    const auto index = build_index({
        {1, "cat"},
        {2, "cat cat cat"},
        {3, "cat cat cat cat cat cat"},
        {4, "dog"},
        {5, "bird"},
    });
    const Ranker ranker(index);

    const auto results = ranker.ranked_or("cat");
    // "cat": df=3 (docs 1,2,3), idf=ln(5/3)
    // Doc 3 (tf=6) > Doc 2 (tf=3) > Doc 1 (tf=1)
    ASSERT_GE(results.size(), 3u);
    EXPECT_EQ(results[0].document_id, 3u);
    EXPECT_EQ(results[1].document_id, 2u);
    EXPECT_EQ(results[2].document_id, 1u);

    // Verify scores are strictly descending
    for (std::size_t i = 1; i < results.size(); ++i) {
        EXPECT_GE(results[i - 1].score, results[i].score);
    }
}

// ===========================================================================
// 5. AND vs OR
// ===========================================================================

TEST(RankerAndVsOr, ANDExcludesPartialMatches)
{
    const auto index = build_index({
        {1, "cat"},
        {2, "dog"},
        {3, "cat dog"},
    });
    const Ranker ranker(index);

    const auto and_results = ranker.ranked_and("cat dog");
    const auto or_results = ranker.ranked_or("cat dog");

    // AND: only doc 3
    ASSERT_EQ(and_results.size(), 1u);
    EXPECT_EQ(and_results[0].document_id, 3u);

    // OR: all three
    ASSERT_EQ(or_results.size(), 3u);
}

TEST(RankerAndVsOr, ANDWithPartialOverlap)
{
    const auto index = build_index({
        {1, "the cat sat"},
        {2, "the dog ran"},
        {3, "the cat and dog"},
        {4, "cat"},
    });
    const Ranker ranker(index);

    const auto and_results = ranker.ranked_and("cat dog");
    ASSERT_EQ(and_results.size(), 1u);
    EXPECT_EQ(and_results[0].document_id, 3u);

    const auto or_results = ranker.ranked_or("cat dog");
    EXPECT_GE(or_results.size(), 3u);
}

// ===========================================================================
// 6. Missing terms
// ===========================================================================

TEST(RankerMissing, ANDWithMissingTerm)
{
    const auto index = build_index({{1, "cat"}, {2, "dog"}});
    const Ranker ranker(index);

    const auto results = ranker.ranked_and("cat nonexistent");
    EXPECT_TRUE(results.empty());
}

TEST(RankerMissing, ORWithMissingTerm)
{
    const auto index = build_index({{1, "cat"}, {2, "dog"}});
    const Ranker ranker(index);

    const auto results = ranker.ranked_or("cat nonexistent");
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].document_id, 1u);
}

TEST(RankerMissing, ANDAllTermsMissing)
{
    const auto index = build_index({{1, "cat"}});
    const Ranker ranker(index);

    EXPECT_TRUE(ranker.ranked_and("xxx yyy").empty());
}

TEST(RankerMissing, ORAllTermsMissing)
{
    const auto index = build_index({{1, "cat"}});
    const Ranker ranker(index);

    EXPECT_TRUE(ranker.ranked_or("xxx yyy").empty());
}

// ===========================================================================
// 7. Empty queries
// ===========================================================================

TEST(RankerEmpty, EmptyString)
{
    const auto index = build_index({{1, "cat"}});
    const Ranker ranker(index);

    EXPECT_TRUE(ranker.ranked_and("").empty());
    EXPECT_TRUE(ranker.ranked_or("").empty());
}

TEST(RankerEmpty, WhitespaceOnly)
{
    const auto index = build_index({{1, "cat"}});
    const Ranker ranker(index);

    EXPECT_TRUE(ranker.ranked_and("   ").empty());
    EXPECT_TRUE(ranker.ranked_or("   ").empty());
}

TEST(RankerEmpty, PunctuationOnly)
{
    const auto index = build_index({{1, "cat"}});
    const Ranker ranker(index);

    EXPECT_TRUE(ranker.ranked_and("!!!").empty());
    EXPECT_TRUE(ranker.ranked_or("...").empty());
}

// ===========================================================================
// 8. Single document corpus
// ===========================================================================

TEST(RankerSingleDoc, AllScoresZero)
{
    const auto index = build_index({{1, "hello world"}});
    const Ranker ranker(index);

    // N=1, df=1 for all terms → idf = ln(1/1) = 0
    const auto results = ranker.ranked_or("hello world");
    ASSERT_EQ(results.size(), 1u);
    EXPECT_DOUBLE_EQ(results[0].score, 0.0);
}

// ===========================================================================
// 9. Determinism
// ===========================================================================

TEST(RankerDeterminism, SameQuerySameResult)
{
    const auto index = build_index({
        {1, "cat dog"},
        {2, "cat"},
        {3, "dog"},
    });
    const Ranker ranker(index);

    const auto r1 = ranker.ranked_or("cat dog");
    const auto r2 = ranker.ranked_or("cat dog");
    const auto r3 = ranker.ranked_or("cat dog");
    EXPECT_EQ(r1, r2);
    EXPECT_EQ(r2, r3);
}

TEST(RankerDeterminism, SameResultsDifferentInsertionOrder)
{
    auto a = build_index({{1, "cat"}, {2, "cat dog"}, {3, "dog"}});
    auto b = build_index({{3, "dog"}, {1, "cat"}, {2, "cat dog"}});

    const Ranker ra(a);
    const Ranker rb(b);

    EXPECT_EQ(ra.ranked_or("cat dog"), rb.ranked_or("cat dog"));
    EXPECT_EQ(ra.ranked_and("cat dog"), rb.ranked_and("cat dog"));
}

// ===========================================================================
// 10. Tie-breaking
// ===========================================================================

TEST(RankerTieBreaking, EqualScoresSortedByDocId)
{
    // N=2, doc 1 has "cat", doc 2 has "dog"
    // Both have tf=1, both terms have df=1, idf=ln(2)
    // Scores are identical → tie-break by doc_id ascending
    const auto index = build_index({{1, "cat"}, {2, "dog"}});
    const Ranker ranker(index);

    const auto results = ranker.ranked_or("cat dog");
    ASSERT_EQ(results.size(), 2u);
    EXPECT_EQ(results[0].document_id, 1u);
    EXPECT_EQ(results[1].document_id, 2u);
}

// ===========================================================================
// 11. End-to-end integration
// ===========================================================================

TEST(RankerIntegration, HandComputedResults)
{
    // Small known corpus
    const auto index = build_index({
        {1, "the cat sat on the mat"},
        {2, "the dog chased the cat"},
        {3, "the cat and the dog played"},
        {4, "a bird sang on the hill"},
        {5, "the mat was blue"},
    });
    const Ranker ranker(index);

    // Query "cat dog"
    const auto and_results = ranker.ranked_and("cat dog");
    // Only docs 2 and 3 have both "cat" and "dog"
    ASSERT_EQ(and_results.size(), 2u);
    // Both docs have tf=1 for "cat" and tf=1 for "dog"
    // "cat": df=3 (docs 1,2,3), idf=ln(5/3)
    // "dog": df=2 (docs 2,3), idf=ln(5/2)
    // Doc 2: 1×ln(5/3) + 1×ln(5/2) = ln(5/3) + ln(5/2)
    // Doc 3: 1×ln(5/3) + 1×ln(5/2) = same
    // Same score → tie-break by doc_id
    EXPECT_EQ(and_results[0].document_id, 2u);
    EXPECT_EQ(and_results[1].document_id, 3u);
    EXPECT_DOUBLE_EQ(and_results[0].score, and_results[1].score);

    // OR query "cat dog"
    const auto or_results = ranker.ranked_or("cat dog");
    EXPECT_GE(or_results.size(), 2u);
    // First result should be doc 2 or 3 (both match both terms)
}

TEST(RankerIntegration, TokenizerNormalizationApplied)
{
    // "Cat" in doc, "cat" in query → must match via tokenizer
    const auto index = build_index({{1, "Cat"}, {2, "CAT"}, {3, "cAt"}});
    const Ranker ranker(index);

    const auto results = ranker.ranked_or("cat");
    // N=3, all docs have "cat" → df=3, idf=0 → all scores 0
    ASSERT_EQ(results.size(), 3u);
    for (const auto& r : results) {
        EXPECT_DOUBLE_EQ(r.score, 0.0);
    }
}

TEST(RankerIntegration, PunctuationInQueryNormalized)
{
    const auto index = build_index({
        {1, "search engine"},
        {2, "search-engine"},
        {3, "SEARCH ENGINE"},
    });
    const Ranker ranker(index);

    // "Search, Engine!" tokenizes to ["search", "engine"]
    const auto results = ranker.ranked_or("Search, Engine!");
    // N=3, "search" df=3 idf=0, "engine" df=3 idf=0 → all scores 0
    ASSERT_EQ(results.size(), 3u);
    for (const auto& r : results) {
        EXPECT_DOUBLE_EQ(r.score, 0.0);
    }
}

TEST(RankerIntegration, HyphenatedQueryTerms)
{
    const auto index = build_index({
        {1, "search engine"},
        {2, "search-engine"},
    });
    const Ranker ranker(index);

    // "search-engine" tokenizes to ["search", "engine"]
    const auto results = ranker.ranked_or("search-engine");
    ASSERT_EQ(results.size(), 2u);
    // N=2, "search" df=2 idf=0, "engine" df=2 idf=0 → all scores 0
    for (const auto& r : results) {
        EXPECT_DOUBLE_EQ(r.score, 0.0);
    }
}

// ===========================================================================
// 12. Edge cases
// ===========================================================================

TEST(RankerEdgeCases, EmptyIndex)
{
    InvertedIndex index;
    const Ranker ranker(index);

    EXPECT_TRUE(ranker.ranked_and("cat").empty());
    EXPECT_TRUE(ranker.ranked_or("cat").empty());
}

TEST(RankerEdgeCases, DocIdZero)
{
    const auto index = build_index({{0, "hello"}, {1, "world"}});
    const Ranker ranker(index);

    const auto results = ranker.ranked_or("hello");
    ASSERT_EQ(results.size(), 1u);
    EXPECT_EQ(results[0].document_id, 0u);
}

TEST(RankerEdgeCases, VeryLongQuery)
{
    InvertedIndex index;
    for (int i = 1; i <= 10; ++i) {
        index.add_document(static_cast<doc_id>(i), "common");
    }
    const Ranker ranker(index);

    // Query with many terms, most missing
    const auto results = ranker.ranked_or("a b c d e");
    EXPECT_TRUE(results.empty());
}

TEST(RankerEdgeCases, ScoreNonNegative)
{
    const auto index = build_index({
        {1, "the quick brown fox"},
        {2, "lazy dog"},
        {3, "the lazy brown fox jumps"},
    });
    const Ranker ranker(index);

    const auto results = ranker.ranked_or("the fox");
    for (const auto& r : results) {
        EXPECT_GE(r.score, 0.0);  // IDF is always >= 0, TF is always >= 1
    }
}

TEST(RankerEdgeCases, ANDNarrowsResults)
{
    const auto index = build_index({
        {1, "alpha beta gamma"},
        {2, "alpha beta"},
        {3, "alpha"},
        {4, "beta gamma"},
    });
    const Ranker ranker(index);

    const auto or_results = ranker.ranked_or("alpha beta");
    const auto and_results = ranker.ranked_and("alpha beta");

    // OR: docs 1, 2, 3, 4 (all have alpha or beta)
    EXPECT_EQ(or_results.size(), 4u);
    // AND: only docs 1, 2 (have both)
    EXPECT_EQ(and_results.size(), 2u);
}

// ===========================================================================
// 13. Large corpus sanity
// ===========================================================================

TEST(RankerLargeCorpus, ManyDocuments)
{
    InvertedIndex index;
    for (int i = 1; i <= 100; ++i) {
        const auto id = static_cast<doc_id>(i);
        if (i % 10 == 0) {
            index.add_document(id, "rare");  // 10 docs have "rare"
        } else {
            index.add_document(id, "common");  // 90 docs have "common"
        }
    }
    const Ranker ranker(index);

    const auto results = ranker.ranked_or("rare");
    ASSERT_EQ(results.size(), 10u);

    // All "rare" docs have same tf and same df → same score
    for (const auto& r : results) {
        EXPECT_DOUBLE_EQ(r.score, std::log(100.0 / 10.0));
    }
}
