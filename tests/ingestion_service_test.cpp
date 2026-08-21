// Distributed Search Engine - Ingestion Service tests (Phase 6B-2).
//
// Tests for dse::IngestionService covering: basic ingestion, validation,
// duplicate rejection, DocumentStore + InvertedIndex coordination,
// empty content, whitespace-only content, multiple documents,
// terms_indexed accuracy, and round-trip ingestion → search.

#include <gtest/gtest.h>

#include "document_store.h"
#include "inverted_index.h"
#include "ingestion_service.h"
#include "tokenizer.h"

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>
#include <thread>
#include <vector>

namespace {

using dse::Document;
using dse::DocumentStore;
using dse::IngestDocumentRequest;
using dse::IngestDocumentResponse;
using dse::IngestionService;
using dse::InvertedIndex;
using dse::Posting;
using dse::doc_id;

} // namespace

// ---------------------------------------------------------------------------
// 1. Basic ingestion
// ---------------------------------------------------------------------------

TEST(IngestionBasic, IngestSingleDocument)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    const auto resp = service.ingest({1, "hello world"});

    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.document_id, 1u);
    EXPECT_EQ(resp.terms_indexed, 2u);  // "hello", "world"
    EXPECT_EQ(store.size(), 1u);
    EXPECT_EQ(index.document_count(), 1u);
}

TEST(IngestionBasic, TermsIndexedIsDistinctCount)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    // "cat cat dog" → 2 distinct terms: "cat", "dog"
    const auto resp = service.ingest({1, "cat cat dog"});

    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.terms_indexed, 2u);
}

// ---------------------------------------------------------------------------
// 2. Validation — empty content
// ---------------------------------------------------------------------------

TEST(IngestionValidation, EmptyContentReturnsError)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    const auto resp = service.ingest({1, ""});

    EXPECT_TRUE(resp.is_error);
    EXPECT_FALSE(resp.error_message.empty());
    EXPECT_EQ(store.size(), 0u);
    EXPECT_EQ(index.document_count(), 0u);
}

// ---------------------------------------------------------------------------
// 3. Validation — whitespace-only content
// ---------------------------------------------------------------------------

TEST(IngestionValidation, WhitespaceOnlyReturnsError)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    const auto resp = service.ingest({1, "   \t\n  "});

    EXPECT_TRUE(resp.is_error);
    EXPECT_EQ(store.size(), 0u);
    EXPECT_EQ(index.document_count(), 0u);
}

// ---------------------------------------------------------------------------
// 4. Duplicate ID rejected
// ---------------------------------------------------------------------------

TEST(IngestionDuplicate, DuplicateIdReturnsError)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    service.ingest({1, "first"});
    const auto resp = service.ingest({1, "second"});

    EXPECT_TRUE(resp.is_error);
    EXPECT_FALSE(resp.error_message.empty());
}

// ---------------------------------------------------------------------------
// 5. Duplicate does not overwrite content
// ---------------------------------------------------------------------------

TEST(IngestionDuplicate, ExistingContentUnchanged)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    service.ingest({42, "original"});
    service.ingest({42, "overwrite attempt"});

    const auto doc = store.get(42);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "original");
}

// ---------------------------------------------------------------------------
// 6. Duplicate does not modify index
// ---------------------------------------------------------------------------

TEST(IngestionDuplicate, IndexUnchangedAfterDuplicate)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    service.ingest({1, "cat cat dog"});
    service.ingest({1, "completely different"});

    // Original document is indexed: "cat" TF=2, "dog" TF=1
    EXPECT_EQ(index.document_count(), 1u);
    EXPECT_EQ(index.postings("cat").size(), 1u);
    EXPECT_EQ(index.postings("cat")[0].term_frequency, 2u);
    EXPECT_EQ(index.postings("dog").size(), 1u);
    EXPECT_EQ(index.postings("dog")[0].term_frequency, 1u);
}

// ---------------------------------------------------------------------------
// 7. Multiple documents
// ---------------------------------------------------------------------------

TEST(IngestionMultiple, DistinctIdsAllStored)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    service.ingest({1, "alpha"});
    service.ingest({2, "beta"});
    service.ingest({3, "gamma"});

    EXPECT_EQ(store.size(), 3u);
    EXPECT_EQ(index.document_count(), 3u);
    EXPECT_TRUE(store.contains(1));
    EXPECT_TRUE(store.contains(2));
    EXPECT_TRUE(store.contains(3));
}

// ---------------------------------------------------------------------------
// 8. DocumentStore and InvertedIndex stay consistent
// ---------------------------------------------------------------------------

TEST(IngestionConsistency, BothStoresHaveSameDocumentCount)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    service.ingest({1, "hello"});
    service.ingest({2, "world"});
    service.ingest({3, "hello world"});

    EXPECT_EQ(store.size(), index.document_count());
}

TEST(IngestionConsistency, PostingsContainIngestedDocuments)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    service.ingest({1, "the cat"});
    service.ingest({2, "the dog"});

    // "the" appears in both documents
    EXPECT_EQ(index.postings("the").size(), 2u);
    // "cat" only in doc 1
    EXPECT_EQ(index.postings("cat").size(), 1u);
    EXPECT_EQ(index.postings("cat")[0].document_id, 1u);
    // "dog" only in doc 2
    EXPECT_EQ(index.postings("dog").size(), 1u);
    EXPECT_EQ(index.postings("dog")[0].document_id, 2u);
}

// ---------------------------------------------------------------------------
// 9. Content validation via static method
// ---------------------------------------------------------------------------

TEST(IngestionValidation, ValidateEmptyContent)
{
    EXPECT_FALSE(IngestionService::validate_request({1, ""}));
}

TEST(IngestionValidation, ValidateWhitespaceContent)
{
    EXPECT_FALSE(IngestionService::validate_request({1, "   "}));
}

TEST(IngestionValidation, ValidateValidContent)
{
    EXPECT_TRUE(IngestionService::validate_request({1, "hello"}));
}

TEST(IngestionValidation, ValidateSingleCharContent)
{
    EXPECT_TRUE(IngestionService::validate_request({1, "x"}));
}

// ---------------------------------------------------------------------------
// 10. terms_indexed accuracy
// ---------------------------------------------------------------------------

TEST(IngestionTermsIndexed, SingleTermDocument)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    const auto resp = service.ingest({1, "hello"});
    EXPECT_EQ(resp.terms_indexed, 1u);
}

TEST(IngestionTermsIndexed, MultipleDistinctTerms)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    // "the cat sat on the mat" → 5 distinct: the, cat, sat, on, mat
    const auto resp = service.ingest({1, "the cat sat on the mat"});
    EXPECT_EQ(resp.terms_indexed, 5u);
}

TEST(IngestionTermsIndexed, DuplicateTokensReduceCount)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    // "cat cat cat" → 1 distinct term: "cat"
    const auto resp = service.ingest({1, "cat cat cat"});
    EXPECT_EQ(resp.terms_indexed, 1u);
}

// ---------------------------------------------------------------------------
// 11. Document content retrievable after ingestion
// ---------------------------------------------------------------------------

TEST(IngestionRetrieval, ContentMatchesOriginal)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    const std::string content = "The quick brown fox jumps over the lazy dog";
    service.ingest({42, content});

    const auto doc = store.get(42);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, content);
}

// ---------------------------------------------------------------------------
// 12. Ingestion followed by search (round-trip)
// ---------------------------------------------------------------------------

TEST(IngestionRoundTrip, IngestedDocumentIsSearchable)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    service.ingest({1, "the cat sat on the mat"});
    service.ingest({2, "the dog chased the cat"});

    // "cat" should appear in both documents
    const auto postings = index.postings("cat");
    EXPECT_EQ(postings.size(), 2u);
}

TEST(IngestionRoundTrip, TermFrequencyCorrectAfterIngestion)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    service.ingest({1, "cat cat dog"});

    const auto cat_postings = index.postings("cat");
    ASSERT_EQ(cat_postings.size(), 1u);
    EXPECT_EQ(cat_postings[0].term_frequency, 2u);

    const auto dog_postings = index.postings("dog");
    ASSERT_EQ(dog_postings.size(), 1u);
    EXPECT_EQ(dog_postings[0].term_frequency, 1u);
}

// ---------------------------------------------------------------------------
// 13. Edge cases
// ---------------------------------------------------------------------------

TEST(IngestionEdgeCases, DocIdZeroIsValid)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    const auto resp = service.ingest({0, "hello"});
    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.document_id, 0u);
    EXPECT_TRUE(store.contains(0));
}

TEST(IngestionEdgeCases, PunctuationOnlyContentRejected)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    // Punctuation-only is technically non-blank, so it's accepted.
    // The tokenizer will produce no tokens, but DocumentStore stores it.
    const auto resp = service.ingest({1, "!!!"});

    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(resp.terms_indexed, 0u);
    EXPECT_TRUE(store.contains(1));
}

TEST(IngestionEdgeCases, LongContentAccepted)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    std::string long_content(10'000, 'x');
    const auto resp = service.ingest({1, long_content});

    EXPECT_FALSE(resp.is_error);
    EXPECT_EQ(store.size(), 1u);
    EXPECT_EQ(index.document_count(), 1u);
}

// ---------------------------------------------------------------------------
// 14. Determinism
// ---------------------------------------------------------------------------

TEST(IngestionDeterminism, SameRequestsSameState)
{
    auto make_state = []() {
        InvertedIndex index;
        DocumentStore store;
        IngestionService service(index, store);
        service.ingest({1, "alpha"});
        service.ingest({2, "beta"});
        service.ingest({3, "gamma"});
        return std::pair{std::move(index), std::move(store)};
    };

    auto [a_idx, a_store] = make_state();
    auto [b_idx, b_store] = make_state();

    EXPECT_EQ(a_idx.document_count(), b_idx.document_count());
    EXPECT_EQ(a_store.size(), b_store.size());
    EXPECT_EQ(a_idx.term_count(), b_idx.term_count());
}

// ===========================================================================
// 15. Concurrency tests (Phase 8B)
// ===========================================================================

// ---------------------------------------------------------------------------
// 15a. Concurrent duplicate ingestion — only one succeeds
// ---------------------------------------------------------------------------

TEST(IngestionConcurrency, ConcurrentDuplicateIngestion)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    constexpr int kThreads = 8;
    std::vector<std::thread> threads;
    std::vector<std::atomic<int>> status_codes(kThreads);
    for (auto& s : status_codes) s.store(0);

    // All threads attempt to ingest the same document ID.
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&service, i, &status_codes]() {
            const auto resp = service.ingest(
                {42, "duplicate attempt " + std::to_string(i)});
            status_codes[i].store(resp.is_error ? 409 : 201);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Exactly one should succeed (201), the rest should fail (409).
    int success_count = 0;
    int conflict_count = 0;
    for (int i = 0; i < kThreads; ++i) {
        const int code = status_codes[i].load();
        if (code == 201) ++success_count;
        else if (code == 409) ++conflict_count;
    }
    EXPECT_EQ(success_count, 1);
    EXPECT_EQ(conflict_count, kThreads - 1);
}

// ---------------------------------------------------------------------------
// 15b. Concurrent unique ingestion — all succeed
// ---------------------------------------------------------------------------

TEST(IngestionConcurrency, ConcurrentUniqueIngestion)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    constexpr int kThreads = 16;
    std::vector<std::thread> threads;
    std::vector<std::atomic<bool>> success(kThreads);
    for (auto& s : success) s.store(false);

    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&service, i, &success]() {
            const auto resp = service.ingest(
                {100 + static_cast<doc_id>(i),
                 "unique document " + std::to_string(i)});
            success[i].store(!resp.is_error);
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // All 16 should succeed (unique IDs).
    for (int i = 0; i < kThreads; ++i) {
        SCOPED_TRACE("thread " + std::to_string(i));
        EXPECT_TRUE(success[i].load());
    }

    EXPECT_EQ(store.size(), static_cast<std::size_t>(kThreads));
    EXPECT_EQ(index.document_count(), static_cast<std::size_t>(kThreads));
}

// ---------------------------------------------------------------------------
// 15c. Duplicate is not indexed twice
// ---------------------------------------------------------------------------

TEST(IngestionConcurrency, DuplicateNotIndexedTwice)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    constexpr int kThreads = 8;
    std::vector<std::thread> threads;

    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&service]() {
            service.ingest({42, "cat cat dog"});
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Document 42 should exist exactly once.
    EXPECT_EQ(store.size(), 1u);
    EXPECT_EQ(index.document_count(), 1u);

    // Term frequencies must match a single ingestion of "cat cat dog".
    const auto cat_postings = index.postings("cat");
    ASSERT_EQ(cat_postings.size(), 1u);
    EXPECT_EQ(cat_postings[0].term_frequency, 2u);
    EXPECT_EQ(cat_postings[0].document_id, 42u);

    const auto dog_postings = index.postings("dog");
    ASSERT_EQ(dog_postings.size(), 1u);
    EXPECT_EQ(dog_postings[0].term_frequency, 1u);
}

// ---------------------------------------------------------------------------
// 15d. Existing content unchanged on concurrent duplicate
// ---------------------------------------------------------------------------

TEST(IngestionConcurrency, ExistingContentUnchangedOnDuplicate)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    // First ingestion sets the content.
    service.ingest({42, "original content"});

    constexpr int kThreads = 8;
    std::vector<std::thread> threads;

    // All threads attempt to overwrite with different content.
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&service, i]() {
            service.ingest({42, "overwrite attempt " + std::to_string(i)});
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // Original content must be preserved.
    const auto doc = store.get(42);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "original content");
}

// ---------------------------------------------------------------------------
// 15e. Document count consistent after concurrent ingestion
// ---------------------------------------------------------------------------

TEST(IngestionConcurrency, DocumentCountConsistentAfterConcurrency)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    constexpr int kThreads = 12;
    std::vector<std::thread> threads;

    // Each thread ingests a unique document.
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back([&service, i]() {
            service.ingest(
                {200 + static_cast<doc_id>(i),
                 "document " + std::to_string(i) + " content"});
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // DocumentStore and InvertedIndex must agree.
    EXPECT_EQ(store.size(), index.document_count());
    EXPECT_EQ(store.size(), static_cast<std::size_t>(kThreads));
}

// ---------------------------------------------------------------------------
// 15f. Concurrent search + ingestion
// ---------------------------------------------------------------------------

TEST(IngestionConcurrency, ConcurrentSearchAndIngestion)
{
    InvertedIndex index;
    DocumentStore store;
    IngestionService service(index, store);

    // Pre-load some documents for searching.
    for (int i = 0; i < 10; ++i) {
        service.ingest({static_cast<doc_id>(i),
                        "searchable term" + std::to_string(i)});
    }

    constexpr int kSearchers = 4;
    constexpr int kIngesters = 4;
    std::vector<std::thread> threads;
    std::atomic<int> successful_searches{0};
    std::atomic<int> successful_ingests{0};

    // Searcher threads.
    for (int i = 0; i < kSearchers; ++i) {
        threads.emplace_back([&index, &successful_searches]() {
            for (int j = 0; j < 5; ++j) {
                const auto postings = index.postings("searchable");
                if (!postings.empty()) {
                    successful_searches.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    // Ingester threads.
    for (int i = 0; i < kIngesters; ++i) {
        threads.emplace_back([&service, i, &successful_ingests]() {
            for (int j = 0; j < 5; ++j) {
                const doc_id id = 1000 + i * 100 + j;
                const auto resp = service.ingest(
                    {id, "concurrent doc " + std::to_string(id)});
                if (!resp.is_error) {
                    successful_ingests.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // All searches should have found results.
    EXPECT_EQ(successful_searches.load(), kSearchers * 5);
    // All ingestions should have succeeded (unique IDs).
    EXPECT_EQ(successful_ingests.load(), kIngesters * 5);
}
