// Distributed Search Engine - Inverted Index tests (Phase 2B).
//
// Real coverage for dse::InvertedIndex per the Phase 2A testing strategy
// (docs/learning/phase-2-inverted-index.md, Section 15). Tests compare the
// COMPLETE returned postings (document ID + term frequency, in order) against
// expected values, never partial checks.
//
// Groups: basic insertion & lookup, term frequency counting, document
// frequency, normalization, empty documents, missing terms, sorted invariant,
// shared terms, counts, determinism, edge cases, span lifetime - plus the
// index <-> tokenizer integration tests.

#include <gtest/gtest.h>

#include "inverted_index.h"
#include "tokenizer.h"

#include <cstdint>
#include <map>
#include <string>
#include <string_view>
#include <vector>

namespace {

using dse::InvertedIndex;
using dse::Posting;

// Compare the full postings list for a term against an expected vector
// (contents + order). Missing terms compare against an empty vector.
void expect_postings(const InvertedIndex& index, std::string_view term,
                     std::vector<Posting> expected)
{
    const auto actual = index.postings(term);
    EXPECT_EQ(actual, expected);
}

// Assert a term has exactly one posting with the given document ID and TF.
void expect_single_posting(const InvertedIndex& index, std::string_view term,
                           dse::doc_id id, std::uint32_t tf)
{
    const auto span = index.postings(term);
    ASSERT_EQ(span.size(), 1u);
    EXPECT_EQ(span[0].document_id, id);
    EXPECT_EQ(span[0].term_frequency, tf);
}

// Verify a postings list is strictly ascending by document ID.
void expect_sorted(const InvertedIndex& index, std::string_view term)
{
    const auto span = index.postings(term);
    for (std::size_t i = 1; i < span.size(); ++i) {
        EXPECT_LT(span[i - 1].document_id, span[i].document_id);
    }
}

// Assert two indices agree on counts and on the postings of every term listed.
void expect_indices_equal(const InvertedIndex& a, const InvertedIndex& b,
                          std::initializer_list<std::string_view> terms)
{
    EXPECT_EQ(a.document_count(), b.document_count());
    EXPECT_EQ(a.term_count(), b.term_count());
    for (const std::string_view term : terms) {
        EXPECT_EQ(a.postings(term), b.postings(term));
    }
}

} // namespace

// ---------------------------------------------------------------------------
// 1. Basic insertion & lookup
// ---------------------------------------------------------------------------

TEST(IndexBasic, SingleWordDocument)
{
    InvertedIndex index;
    index.add_document(1, "hello");

    expect_single_posting(index, "hello", 1, 1);
    EXPECT_EQ(index.document_count(), 1u);
    EXPECT_EQ(index.term_count(), 1u);
}

TEST(IndexBasic, MultipleWordsInDocument)
{
    InvertedIndex index;
    index.add_document(1, "cat sat");

    expect_single_posting(index, "cat", 1, 1);
    expect_single_posting(index, "sat", 1, 1);
    EXPECT_EQ(index.term_count(), 2u);
}

TEST(IndexBasic, MultipleDocumentsDistinctTerms)
{
    InvertedIndex index;
    index.add_document(1, "cat");
    index.add_document(2, "dog");
    index.add_document(3, "bird");

    expect_single_posting(index, "cat", 1, 1);
    expect_single_posting(index, "dog", 2, 1);
    expect_single_posting(index, "bird", 3, 1);
    EXPECT_EQ(index.document_count(), 3u);
    EXPECT_EQ(index.term_count(), 3u);
}

TEST(IndexBasic, MultipleDocumentsSharedTerm)
{
    InvertedIndex index;
    index.add_document(1, "cat");
    index.add_document(2, "cat");
    index.add_document(3, "cat");

    expect_postings(index, "cat", {{1, 1}, {2, 1}, {3, 1}});
    EXPECT_EQ(index.document_count(), 3u);
    EXPECT_EQ(index.term_count(), 1u);
}

// ---------------------------------------------------------------------------
// 2. Term frequency counting
// ---------------------------------------------------------------------------

TEST(IndexTermFrequency, DuplicateTokensAggregateIntoTf)
{
    InvertedIndex index;
    index.add_document(1, "cat cat dog");

    expect_single_posting(index, "cat", 1, 2);
    expect_single_posting(index, "dog", 1, 1);
}

TEST(IndexTermFrequency, ManyDuplicates)
{
    InvertedIndex index;
    index.add_document(1, "the the the the");

    expect_single_posting(index, "the", 1, 4);
    EXPECT_EQ(index.term_count(), 1u);
}

TEST(IndexTermFrequency, MixedRepeatsAcrossTerms)
{
    InvertedIndex index;
    index.add_document(1, "a a b a b");

    expect_single_posting(index, "a", 1, 3);
    expect_single_posting(index, "b", 1, 2);
}

TEST(IndexTermFrequency, TokenOrderWithinDocumentIrrelevant)
{
    InvertedIndex index;
    index.add_document(1, "cat dog cat");

    // Same aggregate result as "cat cat dog": TF counts, not token order.
    expect_single_posting(index, "cat", 1, 2);
    expect_single_posting(index, "dog", 1, 1);
}

// ---------------------------------------------------------------------------
// 3. Document frequency
// ---------------------------------------------------------------------------

TEST(IndexDocumentFrequency, DfIsPostingsListSize)
{
    InvertedIndex index;
    index.add_document(1, "cat");
    index.add_document(2, "cat dog");
    index.add_document(3, "cat dog bird");

    EXPECT_EQ(index.postings("cat").size(), 3u);   // DF("cat") = 3
    EXPECT_EQ(index.postings("dog").size(), 2u);   // DF("dog") = 2
    EXPECT_EQ(index.postings("bird").size(), 1u);  // DF("bird") = 1
}

TEST(IndexDocumentFrequency, DfGrowsAsDocumentsAdded)
{
    InvertedIndex index;
    index.add_document(1, "cat");
    EXPECT_EQ(index.postings("cat").size(), 1u);

    index.add_document(2, "cat");
    EXPECT_EQ(index.postings("cat").size(), 2u);

    index.add_document(3, "cat");
    EXPECT_EQ(index.postings("cat").size(), 3u);
}

TEST(IndexDocumentFrequency, DfOfMissingTermIsZero)
{
    InvertedIndex index;
    index.add_document(1, "cat");

    EXPECT_EQ(index.postings("dog").size(), 0u);
}

// ---------------------------------------------------------------------------
// 4. Normalization
// ---------------------------------------------------------------------------

TEST(IndexNormalization, CaseInsensitiveVocabulary)
{
    InvertedIndex index;
    index.add_document(1, "Cat CAT cat");

    expect_single_posting(index, "cat", 1, 3);
    EXPECT_EQ(index.term_count(), 1u);
}

TEST(IndexNormalization, NoUppercaseTermsStored)
{
    InvertedIndex index;
    index.add_document(1, "HELLO");

    EXPECT_TRUE(index.contains("hello"));
    EXPECT_FALSE(index.contains("HELLO"));
    EXPECT_FALSE(index.contains("Hello"));
}

TEST(IndexNormalization, MixedCaseAcrossDocuments)
{
    InvertedIndex index;
    index.add_document(1, "Search");
    index.add_document(2, "search");

    expect_postings(index, "search", {{1, 1}, {2, 1}});
    EXPECT_EQ(index.term_count(), 1u);
}

TEST(IndexNormalization, UppercaseLookupReturnsEmpty)
{
    InvertedIndex index;
    index.add_document(1, "hello");

    expect_postings(index, "HELLO", {});
    expect_single_posting(index, "hello", 1, 1);
}

// ---------------------------------------------------------------------------
// 5. Empty documents / no tokens
// ---------------------------------------------------------------------------

TEST(IndexEmptyDocuments, EmptyTextAddsNoPostings)
{
    InvertedIndex index;
    index.add_document(1, "");

    EXPECT_EQ(index.document_count(), 1u);
    EXPECT_EQ(index.term_count(), 0u);
    expect_postings(index, "cat", {});
}

TEST(IndexEmptyDocuments, WhitespaceOnlyAddsNoPostings)
{
    InvertedIndex index;
    index.add_document(1, "   \t\n  ");

    EXPECT_EQ(index.document_count(), 1u);
    EXPECT_EQ(index.term_count(), 0u);
}

TEST(IndexEmptyDocuments, PunctuationOnlyAddsNoPostings)
{
    InvertedIndex index;
    index.add_document(1, "!!! ... ---");

    EXPECT_EQ(index.document_count(), 1u);
    EXPECT_EQ(index.term_count(), 0u);
}

TEST(IndexEmptyDocuments, NonAsciiOnlyAddsNoPostings)
{
    InvertedIndex index;
    index.add_document(1, "\xe4\xbd\xa0\xe5\xa5\xbd"); // "你好"

    EXPECT_EQ(index.document_count(), 1u);
    EXPECT_EQ(index.term_count(), 0u);
}

TEST(IndexEmptyDocuments, EmptyDocumentsStillCounted)
{
    InvertedIndex index;
    index.add_document(1, "");
    index.add_document(2, "  ");
    index.add_document(3, "cat");

    EXPECT_EQ(index.document_count(), 3u);
    EXPECT_EQ(index.term_count(), 1u);
    expect_single_posting(index, "cat", 3, 1);
}

// ---------------------------------------------------------------------------
// 6. Missing terms
// ---------------------------------------------------------------------------

TEST(IndexMissingTerms, LookupUnknownTermIsEmpty)
{
    InvertedIndex index;
    index.add_document(1, "cat");

    expect_postings(index, "zzz", {});
    expect_postings(index, "dog", {});
}

TEST(IndexMissingTerms, LookupEmptyStringIsEmpty)
{
    InvertedIndex index;
    index.add_document(1, "cat");

    expect_postings(index, "", {});
}

TEST(IndexMissingTerms, ContainsFalseForUnknownTerm)
{
    InvertedIndex index;
    index.add_document(1, "cat");

    EXPECT_FALSE(index.contains("zzz"));
}

TEST(IndexMissingTerms, FreshIndexLookupIsEmpty)
{
    InvertedIndex index;

    expect_postings(index, "cat", {});
    EXPECT_FALSE(index.contains("cat"));
    EXPECT_EQ(index.document_count(), 0u);
    EXPECT_EQ(index.term_count(), 0u);
}

// ---------------------------------------------------------------------------
// 7. Sorted invariant
// ---------------------------------------------------------------------------

TEST(IndexSortedInvariant, InOrderInsertionAppends)
{
    InvertedIndex index;
    index.add_document(1, "cat");
    index.add_document(2, "cat");
    index.add_document(3, "cat");

    expect_postings(index, "cat", {{1, 1}, {2, 1}, {3, 1}});
}

TEST(IndexSortedInvariant, OutOfOrderInsertionStaysSorted)
{
    InvertedIndex index;
    index.add_document(5, "cat");
    index.add_document(2, "cat");
    index.add_document(9, "cat");

    expect_postings(index, "cat", {{2, 1}, {5, 1}, {9, 1}});
}

TEST(IndexSortedInvariant, OutOfOrderAcrossMultipleTerms)
{
    InvertedIndex index;
    index.add_document(7, "cat dog");
    index.add_document(3, "cat bird");
    index.add_document(5, "dog bird");

    expect_postings(index, "cat", {{3, 1}, {7, 1}});
    expect_postings(index, "dog", {{5, 1}, {7, 1}});
    expect_postings(index, "bird", {{3, 1}, {5, 1}});
}

TEST(IndexSortedInvariant, SortedAfterManyMixedInsertions)
{
    InvertedIndex index;
    // Deliberately shuffled document IDs sharing several terms.
    const std::vector<dse::doc_id> ids = {42, 7, 99, 3, 55, 12, 88, 1, 60, 30};
    for (const dse::doc_id id : ids) {
        index.add_document(id, "alpha beta alpha gamma beta");
    }

    for (const std::string_view term : {"alpha", "beta", "gamma"}) {
        expect_sorted(index, term);
    }
    expect_postings(index, "alpha", {{1, 2}, {3, 2}, {7, 2}, {12, 2}, {30, 2},
                                     {42, 2}, {55, 2}, {60, 2}, {88, 2}, {99, 2}});
}

TEST(IndexSortedInvariant, ReAddingDocumentIdIsContractViolation)
{
    InvertedIndex index;
    index.add_document(1, "hello");

    // ADR-002: re-adding a document ID is a contract violation, asserted in
    // debug builds (assert is active when NDEBUG is not defined).
    EXPECT_DEATH((index.add_document(1, "again")), ".*");
}

// ---------------------------------------------------------------------------
// 8. Multiple terms & shared terms
// ---------------------------------------------------------------------------

TEST(IndexSharedTerms, SharedTermsAcrossDocuments)
{
    InvertedIndex index;
    index.add_document(1, "the cat sat");
    index.add_document(2, "the dog ran");
    index.add_document(3, "the cat and dog");

    expect_postings(index, "the", {{1, 1}, {2, 1}, {3, 1}});
    expect_postings(index, "cat", {{1, 1}, {3, 1}});
    expect_postings(index, "dog", {{2, 1}, {3, 1}});
    expect_single_posting(index, "sat", 1, 1);
    expect_single_posting(index, "ran", 2, 1);
    expect_single_posting(index, "and", 3, 1);
}

TEST(IndexSharedTerms, VocabularySizeCountsDistinctTerms)
{
    InvertedIndex index;
    index.add_document(1, "the cat");
    index.add_document(2, "the dog");
    index.add_document(3, "cat bird");

    EXPECT_EQ(index.term_count(), 4u); // the, cat, dog, bird
}

TEST(IndexSharedTerms, TermPresentInSubsetOfDocuments)
{
    InvertedIndex index;
    index.add_document(1, "alpha beta");
    index.add_document(2, "alpha");
    index.add_document(3, "beta gamma");

    expect_postings(index, "alpha", {{1, 1}, {2, 1}});
    expect_postings(index, "beta", {{1, 1}, {3, 1}});
    expect_single_posting(index, "gamma", 3, 1);
}

TEST(IndexSharedTerms, DisjointDocumentsHaveDisjointPostings)
{
    InvertedIndex index;
    index.add_document(1, "one");
    index.add_document(2, "two");
    index.add_document(3, "three");

    expect_single_posting(index, "one", 1, 1);
    expect_single_posting(index, "two", 2, 1);
    expect_single_posting(index, "three", 3, 1);
    EXPECT_EQ(index.term_count(), 3u);
}

// ---------------------------------------------------------------------------
// 9. Counts
// ---------------------------------------------------------------------------

TEST(IndexCounts, DocumentCountTracksAdds)
{
    InvertedIndex index;
    index.add_document(1, "a");
    index.add_document(2, "a b");
    index.add_document(3, "b c");

    EXPECT_EQ(index.document_count(), 3u);
}

TEST(IndexCounts, TermCountTracksVocabulary)
{
    InvertedIndex index;
    index.add_document(1, "a a a");
    index.add_document(2, "a b b");
    index.add_document(3, "b c");

    EXPECT_EQ(index.term_count(), 3u); // a, b, c
}

TEST(IndexCounts, CountsWithEmptyDocuments)
{
    InvertedIndex index;
    index.add_document(1, "");
    index.add_document(2, "cat");
    index.add_document(3, "  ");
    index.add_document(4, "dog");

    EXPECT_EQ(index.document_count(), 4u);
    EXPECT_EQ(index.term_count(), 2u);
}

// ---------------------------------------------------------------------------
// 10. Determinism
// ---------------------------------------------------------------------------

TEST(IndexDeterminism, IdenticalInsertionSequencesProduceIdenticalIndices)
{
    constexpr std::string_view kTexts[] = {"The cat sat.", "cat cat dog",
                                           "search-engine SEARCH", "42 + 7 = 49"};

    InvertedIndex a;
    InvertedIndex b;
    for (std::size_t i = 0; i < 4; ++i) {
        a.add_document(static_cast<dse::doc_id>(i + 1), kTexts[i]);
        b.add_document(static_cast<dse::doc_id>(i + 1), kTexts[i]);
    }

    expect_indices_equal(a, b, {"the", "cat", "sat", "dog", "search",
                                "engine", "42", "7", "49"});
}

TEST(IndexDeterminism, DifferentInsertionOrdersProduceIdenticalIndices)
{
    // Postings are sorted by document ID regardless of insertion order, so
    // the final index is independent of the order documents arrive in.
    InvertedIndex a;
    a.add_document(1, "cat");
    a.add_document(2, "cat dog");
    a.add_document(3, "dog");

    InvertedIndex b;
    b.add_document(3, "dog");
    b.add_document(1, "cat");
    b.add_document(2, "cat dog");

    expect_indices_equal(a, b, {"cat", "dog"});
}

// ---------------------------------------------------------------------------
// 11. Edge cases
// ---------------------------------------------------------------------------

TEST(IndexEdgeCases, DocIdZeroIsValid)
{
    InvertedIndex index;
    index.add_document(0, "hello");

    expect_single_posting(index, "hello", 0, 1);
    EXPECT_EQ(index.document_count(), 1u);
}

TEST(IndexEdgeCases, MaxDocIdIsValid)
{
    InvertedIndex index;
    constexpr dse::doc_id kMax = UINT32_MAX;
    index.add_document(kMax, "hello");

    expect_single_posting(index, "hello", kMax, 1);
}

TEST(IndexEdgeCases, AlphanumericTermsIndexedAsSingleTerms)
{
    InvertedIndex index;
    index.add_document(1, "abc123 42 007");

    expect_single_posting(index, "abc123", 1, 1);
    expect_single_posting(index, "42", 1, 1);
    expect_single_posting(index, "007", 1, 1);
}

TEST(IndexEdgeCases, TermFrequencyStoredPerDocument)
{
    InvertedIndex index;
    index.add_document(1, "cat");
    index.add_document(2, "cat cat cat");
    index.add_document(3, "cat cat");

    expect_postings(index, "cat", {{1, 1}, {2, 3}, {3, 2}});
}

TEST(IndexEdgeCases, LongDocumentCorrectCounts)
{
    constexpr int kCount = 10'000;

    std::string text;
    text.reserve(6 * static_cast<std::size_t>(kCount));
    for (int i = 0; i < kCount; ++i) {
        text += "search ";
    }

    InvertedIndex index;
    index.add_document(1, text);

    expect_single_posting(index, "search", 1, static_cast<std::uint32_t>(kCount));
    EXPECT_EQ(index.term_count(), 1u);
    EXPECT_EQ(index.document_count(), 1u);
}

TEST(IndexEdgeCases, ManyDocumentsNoCrash)
{
    constexpr int kDocs = 1'000;

    InvertedIndex index;
    for (int i = 0; i < kDocs; ++i) {
        const std::string text = "term" + std::to_string(i) + " common";
        index.add_document(static_cast<dse::doc_id>(i + 1), text);
    }

    EXPECT_EQ(index.document_count(), static_cast<std::size_t>(kDocs));
    EXPECT_EQ(index.term_count(), static_cast<std::size_t>(kDocs) + 1u);
    expect_postings(index, "common",
                    [] {
                        std::vector<Posting> expected;
                        expected.reserve(kDocs);
                        for (int i = 0; i < kDocs; ++i) {
                            expected.push_back(
                                {static_cast<dse::doc_id>(i + 1), 1});
                        }
                        return expected;
                    }());
    expect_sorted(index, "common");
}

TEST(IndexEdgeCases, AddingDocumentsDoesNotAffectOtherTerms)
{
    InvertedIndex index;
    index.add_document(1, "alpha");

    index.add_document(2, "beta gamma");
    index.add_document(3, "delta");

    expect_single_posting(index, "alpha", 1, 1);
    expect_single_posting(index, "beta", 2, 1);
    expect_single_posting(index, "gamma", 2, 1);
    expect_single_posting(index, "delta", 3, 1);
}

// ---------------------------------------------------------------------------
// 12. Span lifetime
// ---------------------------------------------------------------------------

TEST(IndexSpanLifetime, SpanContentsMatchStateAtLookupTime)
{
    InvertedIndex index;
    index.add_document(1, "cat");
    index.add_document(2, "cat");

    const auto span = index.postings("cat");
    ASSERT_EQ(span.size(), 2u);
    EXPECT_EQ(span[0], (Posting{1, 1}));
    EXPECT_EQ(span[1], (Posting{2, 1}));
}

TEST(IndexSpanLifetime, RelookupAfterAddingSameTermReturnsUpdatedList)
{
    InvertedIndex index;
    index.add_document(1, "cat");
    EXPECT_EQ(index.postings("cat").size(), 1u);

    // Modifying the index invalidates previously returned spans; the
    // documented pattern is to re-lookup after each modification.
    index.add_document(2, "cat");
    const auto span = index.postings("cat");
    ASSERT_EQ(span.size(), 2u);
    EXPECT_EQ(span[0], (Posting{1, 1}));
    EXPECT_EQ(span[1], (Posting{2, 1}));
}

TEST(IndexSpanLifetime, RelookupAfterAddingOtherTermKeepsList)
{
    InvertedIndex index;
    index.add_document(1, "cat");

    index.add_document(2, "dog");
    index.add_document(3, "bird");

    // Re-lookup after unrelated modifications: the list is unchanged.
    const auto span = index.postings("cat");
    ASSERT_EQ(span.size(), 1u);
    EXPECT_EQ(span[0], (Posting{1, 1}));
}

TEST(IndexSpanLifetime, CopySurvivesIndexModification)
{
    InvertedIndex index;
    index.add_document(1, "cat");

    // postings() now returns an owning vector snapshot, which is safe to
    // retain across modifications. The returned vector owns its data.
    const auto snapshot = index.postings("cat");

    index.add_document(2, "cat");
    index.add_document(3, "dog");

    EXPECT_EQ(snapshot, (std::vector<Posting>{{1, 1}}));
    EXPECT_EQ(index.postings("cat").size(), 2u);
}

// ===========================================================================
// 14. remove_document (Phase 9)
// ===========================================================================

TEST(IndexRemove, RemoveExistingDocument)
{
    InvertedIndex index;
    index.add_document(1, "cat dog");
    index.add_document(2, "cat bird");

    EXPECT_TRUE(index.remove_document(1));

    EXPECT_EQ(index.document_count(), 1u);
    EXPECT_TRUE(index.contains("cat"));
    EXPECT_FALSE(index.contains("dog"));  // only in doc 1

    // "cat" now only has doc 2
    expect_postings(index, "cat", {{2, 1}});
    // "bird" unaffected
    expect_postings(index, "bird", {{2, 1}});
}

TEST(IndexRemove, RemoveMissingDocumentReturnsFalse)
{
    InvertedIndex index;
    index.add_document(1, "cat");

    EXPECT_FALSE(index.remove_document(2));
    EXPECT_FALSE(index.remove_document(999));
    EXPECT_EQ(index.document_count(), 1u);  // unchanged
}

TEST(IndexRemove, RemoveUpdatesDocumentCount)
{
    InvertedIndex index;
    index.add_document(1, "a");
    index.add_document(2, "b");
    index.add_document(3, "c");

    EXPECT_EQ(index.document_count(), 3u);
    index.remove_document(2);
    EXPECT_EQ(index.document_count(), 2u);
}

TEST(IndexRemove, RemoveCleansPostings)
{
    InvertedIndex index;
    // "dog" only in doc 1
    index.add_document(1, "cat dog");
    // "cat" in both docs
    index.add_document(2, "cat");

    index.remove_document(1);

    // "dog" posting list should be removed entirely
    EXPECT_FALSE(index.contains("dog"));
    EXPECT_EQ(index.postings("dog").size(), 0u);

    // "cat" posting list should only have doc 2
    expect_postings(index, "cat", {{2, 1}});
}

TEST(IndexRemove, RemoveCleansEmptyTerms)
{
    InvertedIndex index;
    // "unique" only in doc 1
    index.add_document(1, "unique common");
    index.add_document(2, "common");

    index.remove_document(1);

    // "unique" should be completely removed from vocabulary
    EXPECT_FALSE(index.contains("unique"));
    EXPECT_EQ(index.term_count(), 1u);  // only "common" remains
}

TEST(IndexRemove, RemovePreservesOtherDocuments)
{
    InvertedIndex index;
    index.add_document(1, "alpha beta");
    index.add_document(2, "beta gamma");
    index.add_document(3, "alpha gamma delta");

    index.remove_document(2);

    expect_postings(index, "alpha", {{1, 1}, {3, 1}});
    expect_postings(index, "beta", {{1, 1}});
    expect_postings(index, "gamma", {{3, 1}});
    expect_single_posting(index, "delta", 3, 1);
    expect_sorted(index, "alpha");
}

TEST(IndexRemove, RemoveAndReAddDocument)
{
    InvertedIndex index;
    index.add_document(1, "cat dog");

    index.remove_document(1);
    EXPECT_EQ(index.document_count(), 0u);

    // Re-add with different content
    index.add_document(1, "bird fish");
    EXPECT_EQ(index.document_count(), 1u);

    expect_single_posting(index, "bird", 1, 1);
    expect_single_posting(index, "fish", 1, 1);
    EXPECT_FALSE(index.contains("cat"));
    EXPECT_FALSE(index.contains("dog"));
}

TEST(IndexRemove, RemoveAllDocumentsLeavesEmptyIndex)
{
    InvertedIndex index;
    index.add_document(1, "alpha");
    index.add_document(2, "beta");
    index.add_document(3, "gamma");

    index.remove_document(1);
    index.remove_document(2);
    index.remove_document(3);

    EXPECT_EQ(index.document_count(), 0u);
    EXPECT_EQ(index.term_count(), 0u);
    EXPECT_FALSE(index.contains("alpha"));
    EXPECT_FALSE(index.contains("beta"));
    EXPECT_FALSE(index.contains("gamma"));
}

TEST(IndexRemove, PostingListsRemainSortedAfterRemoval)
{
    InvertedIndex index;
    for (dse::doc_id i = 1; i <= 10; ++i) {
        index.add_document(i, "common unique" + std::to_string(i));
    }

    // Remove documents 3, 5, 7
    index.remove_document(3);
    index.remove_document(5);
    index.remove_document(7);

    expect_sorted(index, "common");
    expect_sorted(index, "unique3");  // should be empty
    expect_sorted(index, "unique5");  // should be empty
    EXPECT_FALSE(index.contains("unique3"));
    EXPECT_FALSE(index.contains("unique5"));
}

TEST(IndexRemove, RepeatedRemovalIsSafe)
{
    InvertedIndex index;
    index.add_document(1, "cat");

    EXPECT_TRUE(index.remove_document(1));
    EXPECT_FALSE(index.remove_document(1));
    EXPECT_FALSE(index.remove_document(1));
    EXPECT_EQ(index.document_count(), 0u);
}

TEST(IndexRemove, RemoveDocumentWithNoTokens)
{
    InvertedIndex index;
    index.add_document(1, "cat");
    index.add_document(2, "  ");  // no tokens

    EXPECT_TRUE(index.remove_document(2));
    EXPECT_EQ(index.document_count(), 1u);
    EXPECT_EQ(index.term_count(), 1u);
    expect_single_posting(index, "cat", 1, 1);
}

// ---------------------------------------------------------------------------
// 15. Index <-> tokenizer integration
// ---------------------------------------------------------------------------

TEST(IndexTokenizerIntegration, HandComputedPostingsMatchTokenizerOutput)
{
    InvertedIndex index;
    // tokenize("Search-engine, SEARCH!") == ["search", "engine", "search"]
    index.add_document(1, "Search-engine, SEARCH!");

    expect_single_posting(index, "search", 1, 2);
    expect_single_posting(index, "engine", 1, 1);
    EXPECT_EQ(index.term_count(), 2u);
}

TEST(IndexTokenizerIntegration, TfMatchesTokenOccurrenceCount)
{
    const std::string text = "The Cat and the DOG, cat!";
    const auto tokens = dse::tokenize(text);

    // Use the tokenizer as the oracle: every unique token must be a term,
    // with TF equal to its occurrence count in the token list.
    std::map<std::string, std::uint32_t> expected;
    for (const auto& token : tokens) {
        ++expected[token];
    }

    InvertedIndex index;
    index.add_document(1, text);

    EXPECT_EQ(index.term_count(), expected.size());
    for (const auto& [term, tf] : expected) {
        expect_single_posting(index, term, 1, tf);
    }
}

TEST(IndexTokenizerIntegration, NonTokenStringsAreAbsentFromVocabulary)
{
    InvertedIndex index;
    index.add_document(1, "https://example.com/path?q=1");

    // Every token the tokenizer produces for the URL is present...
    for (const auto& term : {"https", "example", "com", "path", "q", "1"}) {
        EXPECT_TRUE(index.contains(term));
    }
    // ...and strings that would only arise from other tokenization rules
    // (hyphens, dots, slashes kept) are not.
    EXPECT_FALSE(index.contains("example.com"));
    EXPECT_FALSE(index.contains("https://example.com"));
    EXPECT_FALSE(index.contains("path?q"));
}
