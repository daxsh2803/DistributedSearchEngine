// Distributed Search Engine - Query Processor tests (Phase 3B).
//
// Real coverage for dse::intersect, dse::merge_union, and
// dse::QueryProcessor per the Phase 3A testing strategy
// (docs/learning/phase-3-query-processing.md, Section 20). Tests compare
// the COMPLETE returned vector: contents, order, and deduplication.
//
// Groups: intersect primitive, union primitive, single-term queries,
// AND multi-term, OR multi-term, missing terms, duplicate query terms,
// empty/punctuation queries, result ordering & determinism, edge cases,
// long-list sanity, tokenizer -> index -> query processor integration.

#include <gtest/gtest.h>

#include "inverted_index.h"
#include "query_processor.h"
#include "tokenizer.h"

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

namespace {

using dse::InvertedIndex;
using dse::Posting;
using dse::QueryProcessor;
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
// 1. Merge primitive — intersect
// ===========================================================================

TEST(IntersectPrimitive, ClassicExampleFromDesignDoc)
{
    // A = [1, 3, 5, 7, 9], B = [2, 3, 5, 8] -> [3, 5]
    const std::vector<Posting> a = {{1, 1}, {3, 1}, {5, 1}, {7, 1}, {9, 1}};
    const std::vector<Posting> b = {{2, 1}, {3, 1}, {5, 1}, {8, 1}};

    EXPECT_EQ(dse::intersect(a, b), (std::vector<doc_id>{3, 5}));
}

TEST(IntersectPrimitive, IdenticalLists)
{
    const std::vector<Posting> a = {{1, 1}, {3, 1}, {5, 1}};
    EXPECT_EQ(dse::intersect(a, a), (std::vector<doc_id>{1, 3, 5}));
}

TEST(IntersectPrimitive, NoOverlap)
{
    const std::vector<Posting> a = {{1, 1}, {2, 1}};
    const std::vector<Posting> b = {{3, 1}, {4, 1}};
    EXPECT_EQ(dse::intersect(a, b), (std::vector<doc_id>{}));
}

TEST(IntersectPrimitive, OneListEmpty)
{
    const std::vector<Posting> a = {{1, 1}, {2, 1}};
    EXPECT_EQ(dse::intersect(a, {}), (std::vector<doc_id>{}));
    EXPECT_EQ(dse::intersect({}, a), (std::vector<doc_id>{}));
}

TEST(IntersectPrimitive, BothEmpty)
{
    EXPECT_EQ(dse::intersect({}, {}), (std::vector<doc_id>{}));
}

TEST(IntersectPrimitive, SingleElementMatch)
{
    const std::vector<Posting> a = {{5, 3}};
    const std::vector<Posting> b = {{5, 1}};
    EXPECT_EQ(dse::intersect(a, b), (std::vector<doc_id>{5}));
}

TEST(IntersectPrimitive, SingleElementNoMatch)
{
    const std::vector<Posting> a = {{3, 1}};
    const std::vector<Posting> b = {{5, 1}};
    EXPECT_EQ(dse::intersect(a, b), (std::vector<doc_id>{}));
}

TEST(IntersectPrimitive, Subset)
{
    // [1,2,3,4,5] ∩ [2,4] -> [2,4]
    const std::vector<Posting> a = {{1, 1}, {2, 1}, {3, 1}, {4, 1}, {5, 1}};
    const std::vector<Posting> b = {{2, 1}, {4, 1}};
    EXPECT_EQ(dse::intersect(a, b), (std::vector<doc_id>{2, 4}));
}

TEST(IntersectPrimitive, ResultDoesNotCarryTf)
{
    const std::vector<Posting> a = {{1, 5}, {3, 2}};
    const std::vector<Posting> b = {{1, 3}, {3, 7}};
    const auto result = dse::intersect(a, b);
    // Only docIDs are returned, not postings.
    EXPECT_EQ(result, (std::vector<doc_id>{1, 3}));
}

// ===========================================================================
// 2. Merge primitive — union
// ===========================================================================

TEST(UnionPrimitive, ClassicExampleFromDesignDoc)
{
    const std::vector<Posting> a = {{1, 1}, {3, 1}, {5, 1}, {7, 1}, {9, 1}};
    const std::vector<Posting> b = {{2, 1}, {3, 1}, {5, 1}, {8, 1}};
    EXPECT_EQ(dse::merge_union(a, b),
              (std::vector<doc_id>{1, 2, 3, 5, 7, 8, 9}));
}

TEST(UnionPrimitive, IdenticalLists)
{
    const std::vector<Posting> a = {{1, 1}, {3, 1}, {5, 1}};
    EXPECT_EQ(dse::merge_union(a, a), (std::vector<doc_id>{1, 3, 5}));
}

TEST(UnionPrimitive, DisjointLists)
{
    const std::vector<Posting> a = {{1, 1}, {3, 1}};
    const std::vector<Posting> b = {{2, 1}, {4, 1}};
    EXPECT_EQ(dse::merge_union(a, b), (std::vector<doc_id>{1, 2, 3, 4}));
}

TEST(UnionPrimitive, OneListEmpty)
{
    const std::vector<Posting> a = {{1, 1}, {2, 1}};
    EXPECT_EQ(dse::merge_union(a, {}), (std::vector<doc_id>{1, 2}));
    EXPECT_EQ(dse::merge_union({}, a), (std::vector<doc_id>{1, 2}));
}

TEST(UnionPrimitive, BothEmpty)
{
    EXPECT_EQ(dse::merge_union({}, {}), (std::vector<doc_id>{}));
}

TEST(UnionPrimitive, SharedElementsDeduped)
{
    const std::vector<Posting> a = {{1, 1}, {2, 1}, {3, 1}};
    const std::vector<Posting> b = {{2, 1}, {3, 1}, {4, 1}};
    EXPECT_EQ(dse::merge_union(a, b), (std::vector<doc_id>{1, 2, 3, 4}));
}

TEST(UnionPrimitive, AdjacentDocIds)
{
    const std::vector<Posting> a = {{1, 1}};
    const std::vector<Posting> b = {{2, 1}};
    EXPECT_EQ(dse::merge_union(a, b), (std::vector<doc_id>{1, 2}));
}

// ===========================================================================
// 3. Single-term queries
// ===========================================================================

TEST(QuerySingleTerm, PresentTerm)
{
    const auto index = build_index({{1, "cat"}, {2, "dog"}, {3, "cat"}});
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.and_query("cat"), (std::vector<doc_id>{1, 3}));
    EXPECT_EQ(qp.or_query("cat"), (std::vector<doc_id>{1, 3}));
}

TEST(QuerySingleTerm, AbsentTerm)
{
    const auto index = build_index({{1, "cat"}});
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.and_query("dog"), (std::vector<doc_id>{}));
    EXPECT_EQ(qp.or_query("dog"), (std::vector<doc_id>{}));
}

TEST(QuerySingleTerm, MultipleDocumentsSameTerm)
{
    const auto index = build_index(
        {{1, "hello"}, {2, "hello"}, {3, "hello"}, {4, "world"}});
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.or_query("hello"), (std::vector<doc_id>{1, 2, 3}));
}

TEST(QuerySingleTerm, EmptyIndex)
{
    InvertedIndex index;
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.and_query("anything"), (std::vector<doc_id>{}));
    EXPECT_EQ(qp.or_query("anything"), (std::vector<doc_id>{}));
}

// ===========================================================================
// 4. AND multi-term
// ===========================================================================

TEST(QueryAnd, TwoTermsSharedDocs)
{
    const auto index = build_index({
        {1, "cat"},
        {2, "dog"},
        {3, "cat and dog"},
    });
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.and_query("cat dog"), (std::vector<doc_id>{3}));
}

TEST(QueryAnd, TwoTermsNoOverlap)
{
    const auto index = build_index({
        {1, "cat"},
        {2, "dog"},
    });
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.and_query("cat dog"), (std::vector<doc_id>{}));
}

TEST(QueryAnd, ThreeTermsPartialOverlap)
{
    const auto index = build_index({
        {1, "the cat sat"},
        {2, "the dog ran"},
        {3, "the cat and dog"},
    });
    const QueryProcessor qp(index);

    // "the cat" appears in docs 1 and 3; "dog" in 2 and 3; all three in 3.
    EXPECT_EQ(qp.and_query("the cat dog"), (std::vector<doc_id>{3}));
}

TEST(QueryAnd, ThreeTermsAllShared)
{
    const auto index = build_index({
        {1, "alpha beta gamma"},
        {2, "alpha beta gamma"},
    });
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.and_query("alpha beta gamma"),
              (std::vector<doc_id>{1, 2}));
}

TEST(QueryAnd, TwoTermsBothInMultipleDocs)
{
    const auto index = build_index({
        {1, "cat"},
        {2, "cat dog"},
        {3, "dog"},
        {4, "cat dog"},
    });
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.and_query("cat dog"), (std::vector<doc_id>{2, 4}));
}

TEST(QueryAnd, ResultSorted)
{
    // Insert in reverse order to verify sort is not dependent on insertion.
    const auto index = build_index({
        {10, "alpha beta"},
        {3, "alpha beta"},
        {7, "alpha beta"},
        {1, "alpha beta"},
    });
    const QueryProcessor qp(index);

    const auto result = qp.and_query("alpha beta");
    EXPECT_EQ(result, (std::vector<doc_id>{1, 3, 7, 10}));
}

// ===========================================================================
// 5. OR multi-term
// ===========================================================================

TEST(QueryOr, TwoTermsOverlap)
{
    const auto index = build_index({
        {1, "cat"},
        {2, "dog"},
        {3, "cat and dog"},
    });
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.or_query("cat dog"), (std::vector<doc_id>{1, 2, 3}));
}

TEST(QueryOr, TwoTermsDisjoint)
{
    const auto index = build_index({
        {1, "cat"},
        {2, "dog"},
        {3, "bird"},
    });
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.or_query("cat dog"), (std::vector<doc_id>{1, 2}));
}

TEST(QueryOr, ThreeTermsUnion)
{
    const auto index = build_index({
        {1, "alpha"},
        {2, "beta"},
        {3, "gamma"},
        {4, "alpha beta"},
    });
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.or_query("alpha beta gamma"),
              (std::vector<doc_id>{1, 2, 3, 4}));
}

TEST(QueryOr, DeduplicatesSharedDocIds)
{
    const auto index = build_index({
        {1, "cat dog"},
        {2, "cat"},
        {3, "dog"},
    });
    const QueryProcessor qp(index);

    // Doc 1 appears in both "cat" and "dog" postings; emitted once.
    EXPECT_EQ(qp.or_query("cat dog"), (std::vector<doc_id>{1, 2, 3}));
}

TEST(QueryOr, ResultSorted)
{
    const auto index = build_index({
        {10, "x"},
        {3, "y"},
        {7, "x y"},
        {1, "y"},
    });
    const QueryProcessor qp(index);

    const auto result = qp.or_query("x y");
    EXPECT_EQ(result, (std::vector<doc_id>{1, 3, 7, 10}));
}

// ===========================================================================
// 6. Missing terms
// ===========================================================================

TEST(QueryMissingTerms, ANDWithOneMissing)
{
    const auto index = build_index({
        {1, "cat"},
        {2, "cat dog"},
    });
    const QueryProcessor qp(index);

    // "zzz" is missing: AND should be empty.
    EXPECT_EQ(qp.and_query("cat zzz"), (std::vector<doc_id>{}));
}

TEST(QueryMissingTerms, ORWithOneMissing)
{
    const auto index = build_index({
        {1, "cat"},
        {2, "cat dog"},
    });
    const QueryProcessor qp(index);

    // "zzz" is missing: OR returns only "cat" matches.
    EXPECT_EQ(qp.or_query("cat zzz"), (std::vector<doc_id>{1, 2}));
}

TEST(QueryMissingTerms, ANDAllMissing)
{
    const auto index = build_index({{1, "cat"}});
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.and_query("zzz www"), (std::vector<doc_id>{}));
}

TEST(QueryMissingTerms, ORAllMissing)
{
    const auto index = build_index({{1, "cat"}});
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.or_query("zzz www"), (std::vector<doc_id>{}));
}

TEST(QueryMissingTerms, ANDWithBothTermsMissing)
{
    const auto index = build_index({{1, "alpha"}, {2, "beta"}});
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.and_query("gamma delta"), (std::vector<doc_id>{}));
}

// ===========================================================================
// 7. Duplicate query terms
// ===========================================================================

TEST(QueryDuplicateTerms, ANDWithDuplicate)
{
    const auto index = build_index({
        {1, "cat"},
        {2, "cat dog"},
    });
    const QueryProcessor qp(index);

    // "cat cat dog" should give the same result as "cat dog".
    EXPECT_EQ(qp.and_query("cat cat dog"), (std::vector<doc_id>{2}));
}

TEST(QueryDuplicateTerms, ORWithDuplicate)
{
    const auto index = build_index({
        {1, "cat"},
        {2, "dog"},
    });
    const QueryProcessor qp(index);

    // "cat cat dog" should give the same result as "cat dog" — no duplicate
    // docIDs in the output.
    EXPECT_EQ(qp.or_query("cat cat dog"), (std::vector<doc_id>{1, 2}));
}

TEST(QueryDuplicateTerms, AllSameTerm)
{
    const auto index = build_index({
        {1, "cat"},
        {2, "cat cat"},
    });
    const QueryProcessor qp(index);

    // "cat cat cat" deduplicates to just "cat".
    EXPECT_EQ(qp.and_query("cat cat cat"), (std::vector<doc_id>{1, 2}));
    EXPECT_EQ(qp.or_query("cat cat cat"), (std::vector<doc_id>{1, 2}));
}

// ===========================================================================
// 8. Empty / punctuation-only queries
// ===========================================================================

TEST(QueryEmpty, EmptyString)
{
    const auto index = build_index({{1, "cat"}, {2, "dog"}});
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.and_query(""), (std::vector<doc_id>{}));
    EXPECT_EQ(qp.or_query(""), (std::vector<doc_id>{}));
}

TEST(QueryEmpty, WhitespaceOnly)
{
    const auto index = build_index({{1, "cat"}, {2, "dog"}});
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.and_query("   "), (std::vector<doc_id>{}));
    EXPECT_EQ(qp.or_query("   "), (std::vector<doc_id>{}));
}

TEST(QueryEmpty, PunctuationOnly)
{
    const auto index = build_index({{1, "cat"}, {2, "dog"}});
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.and_query("!!!"), (std::vector<doc_id>{}));
    EXPECT_EQ(qp.or_query("..."), (std::vector<doc_id>{}));
    EXPECT_EQ(qp.and_query("---"), (std::vector<doc_id>{}));
}

TEST(QueryEmpty, NonAsciiOnly)
{
    const auto index = build_index({{1, "cat"}, {2, "dog"}});
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.and_query("\xe4\xbd\xa0\xe5\xa5\xbd"),
              (std::vector<doc_id>{}));
    EXPECT_EQ(qp.or_query("\xe4\xbd\xa0\xe5\xa5\xbd"),
              (std::vector<doc_id>{}));
}

// ===========================================================================
// 9. Result ordering & determinism
// ===========================================================================

TEST(QueryOrdering, ResultsAlwaysSortedAscending)
{
    const auto index = build_index({
        {100, "alpha"},
        {1, "alpha"},
        {50, "alpha"},
        {25, "alpha"},
    });
    const QueryProcessor qp(index);

    const auto result = qp.or_query("alpha");
    EXPECT_EQ(result, (std::vector<doc_id>{1, 25, 50, 100}));
}

TEST(QueryDeterminism, SameQuerySameResult)
{
    const auto index = build_index({
        {1, "cat dog"},
        {2, "cat"},
        {3, "dog"},
    });
    const QueryProcessor qp(index);

    const auto r1 = qp.and_query("cat dog");
    const auto r2 = qp.and_query("cat dog");
    const auto r3 = qp.and_query("cat dog");
    EXPECT_EQ(r1, r2);
    EXPECT_EQ(r2, r3);
}

TEST(QueryDeterminism, SameQueryDifferentIndices)
{
    // Build two identical indices in different insertion order.
    auto a = build_index({{1, "cat"}, {2, "cat dog"}, {3, "dog"}});
    auto b = build_index({{3, "dog"}, {1, "cat"}, {2, "cat dog"}});

    const QueryProcessor qa(a);
    const QueryProcessor qb(b);

    EXPECT_EQ(qa.and_query("cat dog"), qb.and_query("cat dog"));
    EXPECT_EQ(qa.or_query("cat dog"), qb.or_query("cat dog"));
}

// ===========================================================================
// 10. Edge cases
// ===========================================================================

TEST(QueryEdgeCases, DocIdZero)
{
    const auto index = build_index({{0, "hello"}, {1, "world"}});
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.or_query("hello"), (std::vector<doc_id>{0}));
}

TEST(QueryEdgeCases, AdjacentDocIds)
{
    const auto index = build_index({
        {1, "cat"},
        {2, "cat"},
        {3, "cat"},
    });
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.or_query("cat"), (std::vector<doc_id>{1, 2, 3}));
}

TEST(QueryEdgeCases, FirstAndLastElementsShared)
{
    const std::vector<Posting> a = {{1, 1}, {3, 1}, {5, 1}, {7, 1}, {9, 1}};
    const std::vector<Posting> b = {{1, 1}, {2, 1}, {4, 1}, {9, 1}};
    EXPECT_EQ(dse::intersect(a, b), (std::vector<doc_id>{1, 9}));
}

TEST(QueryEdgeCases, TermAppearsInEveryDocument)
{
    InvertedIndex index;
    for (int i = 1; i <= 10; ++i) {
        index.add_document(static_cast<doc_id>(i), "common");
    }
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.and_query("common"),
              (std::vector<doc_id>{1, 2, 3, 4, 5, 6, 7, 8, 9, 10}));
}

TEST(QueryEdgeCases, QueryLongerThanAnyTerm)
{
    // "search engine ranking distributed" has 4 terms; the index may have
    // fewer.
    const auto index = build_index({
        {1, "search"},
        {2, "engine"},
    });
    const QueryProcessor qp(index);

    // AND: all 4 terms must match, but "ranking" and "distributed" are missing.
    EXPECT_EQ(qp.and_query("search engine ranking distributed"),
              (std::vector<doc_id>{}));
    // OR: only existing terms contribute.
    EXPECT_EQ(qp.or_query("search engine ranking distributed"),
              (std::vector<doc_id>{1, 2}));
}

// ===========================================================================
// 11. Long-list sanity
// ===========================================================================

TEST(QueryLongLists, IntersectLargeLists)
{
    // Two lists of 1000 docIDs each, sharing every other one.
    InvertedIndex index;
    for (int i = 0; i < 2000; ++i) {
        const auto id = static_cast<doc_id>(i + 1);
        std::string text;
        if (i % 2 == 0) {
            text = "alpha beta";  // in both
        } else if (i % 2 == 1 && i < 1000) {
            text = "alpha";       // only in alpha
        } else {
            text = "beta";        // only in beta
        }
        index.add_document(id, text);
    }

    const QueryProcessor qp(index);
    const auto result = qp.and_query("alpha beta");

    // Every even-numbered doc has both terms.
    std::vector<doc_id> expected;
    for (int i = 0; i < 2000; i += 2) {
        expected.push_back(static_cast<doc_id>(i + 1));
    }
    EXPECT_EQ(result, expected);
}

TEST(QueryLongLists, UnionLargeLists)
{
    InvertedIndex index;
    for (int i = 0; i < 1000; ++i) {
        index.add_document(static_cast<doc_id>(i + 1), "alpha");
    }
    for (int i = 1000; i < 2000; ++i) {
        index.add_document(static_cast<doc_id>(i + 1), "beta");
    }

    const QueryProcessor qp(index);
    const auto result = qp.or_query("alpha beta");

    // All 2000 documents should appear.
    EXPECT_EQ(result.size(), 2000u);
    // Verify sorted.
    for (std::size_t i = 1; i < result.size(); ++i) {
        EXPECT_LT(result[i - 1], result[i]);
    }
}

// ===========================================================================
// 12. End-to-end integration: tokenizer -> index -> query processor
// ===========================================================================

TEST(QueryIntegration, HandComputedResults)
{
    // Build a small known corpus.
    const auto index = build_index({
        {1, "the cat sat on the mat"},
        {2, "the dog chased the cat"},
        {3, "the cat and the dog played"},
        {4, "a bird sang on the hill"},
        {5, "the mat was blue"},
    });
    const QueryProcessor qp(index);

    // AND "cat dog": docs 2 and 3 have both.
    EXPECT_EQ(qp.and_query("cat dog"), (std::vector<doc_id>{2, 3}));

    // OR "cat dog": docs 1, 2, 3 have either (4, 5 don't have cat or dog).
    EXPECT_EQ(qp.or_query("cat dog"), (std::vector<doc_id>{1, 2, 3}));

    // AND "the cat": docs 1, 2, 3 (all have "the" and "cat"; doc 4 has "the"
    // but not "cat"; doc 5 has "the" but not "cat").
    EXPECT_EQ(qp.and_query("the cat"), (std::vector<doc_id>{1, 2, 3}));

    // AND "cat mat": only doc 1 has both.
    EXPECT_EQ(qp.and_query("cat mat"), (std::vector<doc_id>{1}));

    // OR "bird hill": only doc 4.
    EXPECT_EQ(qp.or_query("bird hill"), (std::vector<doc_id>{4}));

    // AND "the bird mat": doc 4 has "the" and "bird" but not "mat";
    // doc 1 has "the" and "mat" but not "bird"; doc 5 has "the" and "mat"
    // but not "bird". Empty.
    EXPECT_EQ(qp.and_query("the bird mat"), (std::vector<doc_id>{}));

    // OR "bird mat hill": docs 4 and 5. (4 has bird+mat+hill; 5 has mat.)
    EXPECT_EQ(qp.or_query("bird mat hill"), (std::vector<doc_id>{1, 4, 5}));
}

TEST(QueryIntegration, TokenizerNormalizationApplied)
{
    // "Cat" in document, "cat" in query — must match via tokenizer.
    const auto index = build_index({{1, "Cat"}, {2, "CAT"}, {3, "cAt"}});
    const QueryProcessor qp(index);

    // All three docs have "cat" after tokenization.
    EXPECT_EQ(qp.or_query("cat"), (std::vector<doc_id>{1, 2, 3}));
}

TEST(QueryIntegration, PunctuationInQueryNormalized)
{
    const auto index = build_index({
        {1, "search engine"},
        {2, "search-engine"},
        {3, "SEARCH ENGINE"},
    });
    const QueryProcessor qp(index);

    // "Search, Engine!" tokenizes to ["search", "engine"] — matches all three.
    EXPECT_EQ(qp.and_query("Search, Engine!"),
              (std::vector<doc_id>{1, 2, 3}));
}

TEST(QueryIntegration, HyphenatedQueryTerms)
{
    const auto index = build_index({
        {1, "search engine"},
        {2, "search-engine"},
    });
    const QueryProcessor qp(index);

    // "search-engine" tokenizes to ["search", "engine"].
    EXPECT_EQ(qp.and_query("search-engine"), (std::vector<doc_id>{1, 2}));
}

TEST(QueryIntegration, EmptyIndexQuery)
{
    InvertedIndex index;
    const QueryProcessor qp(index);

    EXPECT_EQ(qp.and_query("anything"), (std::vector<doc_id>{}));
    EXPECT_EQ(qp.or_query("anything"), (std::vector<doc_id>{}));
}

TEST(QueryIntegration, LargeCorpusConsistency)
{
    // 100 documents, each containing a unique term plus "common".
    InvertedIndex index;
    for (int i = 0; i < 100; ++i) {
        const auto id = static_cast<doc_id>(i + 1);
        const std::string text = "term" + std::to_string(i) + " common";
        index.add_document(id, text);
    }

    const QueryProcessor qp(index);

    // Single term: all 100 docs.
    EXPECT_EQ(qp.or_query("common").size(), 100u);

    // AND "common term0": only doc 1.
    EXPECT_EQ(qp.and_query("common term0"), (std::vector<doc_id>{1}));

    // OR "term0 term1 term2": docs 1, 2, 3.
    EXPECT_EQ(qp.or_query("term0 term1 term2"),
              (std::vector<doc_id>{1, 2, 3}));
}
