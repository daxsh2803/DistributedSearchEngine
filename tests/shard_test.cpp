// Distributed Search Engine - Shard Tests (Phase 10B).
//
// Tests for dse::Shard: lifecycle operations, persistence, local search,
// and the DocumentStore/InvertedIndex invariant.

#include "shard.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

namespace dse {
namespace {

// =========================================================================
// 1. Basic add / get / contains / size
// =========================================================================

TEST(ShardTest, AddAndGetDocument)
{
    Shard shard;
    EXPECT_TRUE(shard.add_document(1, "hello world"));
    EXPECT_EQ(shard.document_count(), 1u);

    const auto doc = shard.get_document(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->id, 1u);
    EXPECT_EQ(doc->content, "hello world");
}

TEST(ShardTest, AddDuplicateDocumentFails)
{
    Shard shard;
    EXPECT_TRUE(shard.add_document(1, "first"));
    EXPECT_FALSE(shard.add_document(1, "second"));
    EXPECT_EQ(shard.document_count(), 1u);

    // Original content preserved.
    const auto doc = shard.get_document(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "first");
}

TEST(ShardTest, ContainsExistingDocument)
{
    Shard shard;
    shard.add_document(42, "test");
    EXPECT_TRUE(shard.contains_document(42));
}

TEST(ShardTest, ContainsMissingDocument)
{
    Shard shard;
    EXPECT_FALSE(shard.contains_document(999));
}

TEST(ShardTest, GetMissingDocumentReturnsNullopt)
{
    Shard shard;
    EXPECT_FALSE(shard.get_document(1).has_value());
}

TEST(ShardTest, MultipleDocuments)
{
    Shard shard;
    shard.add_document(1, "alpha");
    shard.add_document(2, "beta");
    shard.add_document(3, "gamma");
    EXPECT_EQ(shard.document_count(), 3u);
    EXPECT_TRUE(shard.contains_document(1));
    EXPECT_TRUE(shard.contains_document(2));
    EXPECT_TRUE(shard.contains_document(3));
}

// =========================================================================
// 2. Content validation
// =========================================================================

TEST(ShardTest, EmptyContentRejected)
{
    Shard shard;
    EXPECT_FALSE(shard.add_document(1, ""));
    EXPECT_EQ(shard.document_count(), 0u);
}

TEST(ShardTest, WhitespaceOnlyContentRejected)
{
    Shard shard;
    EXPECT_FALSE(shard.add_document(1, "   \t\n  "));
    EXPECT_EQ(shard.document_count(), 0u);
}

TEST(ShardTest, EmptyContentUpdateRejected)
{
    Shard shard;
    shard.add_document(1, "original");
    EXPECT_FALSE(shard.update_document(1, ""));
    // Original content unchanged.
    const auto doc = shard.get_document(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "original");
}

// =========================================================================
// 3. Update
// =========================================================================

TEST(ShardTest, UpdateExistingDocument)
{
    Shard shard;
    shard.add_document(1, "original");
    EXPECT_TRUE(shard.update_document(1, "updated"));
    EXPECT_EQ(shard.document_count(), 1u);

    const auto doc = shard.get_document(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "updated");
}

TEST(ShardTest, UpdateMissingDocumentFails)
{
    Shard shard;
    EXPECT_FALSE(shard.update_document(999, "new content"));
}

TEST(ShardTest, UpdatePreservesDocumentCount)
{
    Shard shard;
    shard.add_document(1, "a");
    shard.add_document(2, "b");
    const auto count_before = shard.document_count();
    shard.update_document(1, "new a");
    EXPECT_EQ(shard.document_count(), count_before);
}

TEST(ShardTest, OldTermsDisappearAfterUpdate)
{
    Shard shard;
    shard.add_document(1, "cat dog");

    // "cat" is in the index.
    EXPECT_TRUE(shard.index().contains("cat"));

    shard.update_document(1, "bird fish");

    // "cat" should no longer be findable via this document.
    // The index should reflect the new terms.
    EXPECT_TRUE(shard.index().contains("bird"));
    EXPECT_TRUE(shard.index().contains("fish"));
    EXPECT_FALSE(shard.index().contains("cat"));
}

// =========================================================================
// 4. Delete
// =========================================================================

TEST(ShardTest, DeleteExistingDocument)
{
    Shard shard;
    shard.add_document(1, "to be deleted");
    EXPECT_TRUE(shard.remove_document(1));
    EXPECT_EQ(shard.document_count(), 0u);
    EXPECT_FALSE(shard.contains_document(1));
}

TEST(ShardTest, DeleteMissingDocumentFails)
{
    Shard shard;
    EXPECT_FALSE(shard.remove_document(999));
}

TEST(ShardTest, DeleteDecreasesDocumentCount)
{
    Shard shard;
    shard.add_document(1, "a");
    shard.add_document(2, "b");
    shard.add_document(3, "c");
    const auto count_before = shard.document_count();
    shard.remove_document(2);
    EXPECT_EQ(shard.document_count(), count_before - 1);
}

TEST(ShardTest, DeletedDocumentNotInIndex)
{
    Shard shard;
    shard.add_document(1, "hello world");
    EXPECT_TRUE(shard.index().contains("hello"));

    shard.remove_document(1);

    // After deletion, "hello" posting list should be empty.
    const auto postings = shard.index().postings("hello");
    EXPECT_TRUE(postings.empty());
}

TEST(ShardTest, DeletePreservesOtherDocuments)
{
    Shard shard;
    shard.add_document(1, "alpha beta");
    shard.add_document(2, "gamma delta");
    shard.remove_document(1);

    EXPECT_FALSE(shard.contains_document(1));
    EXPECT_TRUE(shard.contains_document(2));
    EXPECT_EQ(shard.document_count(), 1u);

    // "gamma" still in index from doc 2.
    EXPECT_TRUE(shard.index().contains("gamma"));
}

// =========================================================================
// 5. Local search (verify index is usable)
// =========================================================================

TEST(ShardTest, SearchFindsAddedDocuments)
{
    Shard shard;
    shard.add_document(1, "quick brown fox");
    shard.add_document(2, "lazy dog");
    shard.add_document(3, "another fox");

    // "fox" should be in 2 documents.
    const auto postings = shard.index().postings("fox");
    EXPECT_EQ(postings.size(), 2u);
}

TEST(ShardTest, SearchReflectsUpdates)
{
    Shard shard;
    shard.add_document(1, "cat dog");

    auto p1 = shard.index().postings("cat");
    EXPECT_EQ(p1.size(), 1u);

    shard.update_document(1, "bird fish");

    auto p2 = shard.index().postings("cat");
    EXPECT_TRUE(p2.empty());

    auto p3 = shard.index().postings("bird");
    EXPECT_EQ(p3.size(), 1u);
}

TEST(ShardTest, SearchReflectsDeletions)
{
    Shard shard;
    shard.add_document(1, "unique_term_xyz");
    EXPECT_EQ(shard.index().document_count(), 1u);

    shard.remove_document(1);
    EXPECT_EQ(shard.index().document_count(), 0u);

    const auto postings = shard.index().postings("unique_term_xyz");
    EXPECT_TRUE(postings.empty());
}

// =========================================================================
// 6. Persistence
// =========================================================================

class ShardPersistenceTest : public ::testing::Test {
protected:
    std::string path_;

    void SetUp() override {
        // Use a unique temp path for each test.
        path_ = "test_shard_persist_" + std::to_string(
            reinterpret_cast<std::uintptr_t>(this)) + ".jsonl";
    }

    void TearDown() override {
        std::remove(path_.c_str());
    }
};

TEST_F(ShardPersistenceTest, SaveAndLoadRoundTrip)
{
    {
        Shard shard(path_);
        shard.add_document(1, "hello world");
        shard.add_document(2, "foo bar");
        EXPECT_TRUE(shard.save());
    }

    {
        Shard loaded(path_);
        EXPECT_TRUE(loaded.load());
        EXPECT_EQ(loaded.document_count(), 2u);
        EXPECT_TRUE(loaded.contains_document(1));
        EXPECT_TRUE(loaded.contains_document(2));

        const auto doc1 = loaded.get_document(1);
        ASSERT_TRUE(doc1.has_value());
        EXPECT_EQ(doc1->content, "hello world");
    }
}

TEST_F(ShardPersistenceTest, LoadRebuildsIndex)
{
    {
        Shard shard(path_);
        shard.add_document(1, "cat dog");
        shard.add_document(2, "cat bird");
        EXPECT_TRUE(shard.save());
    }

    {
        Shard loaded(path_);
        EXPECT_TRUE(loaded.load());

        // Index should be rebuilt: "cat" appears in 2 documents.
        const auto cat_postings = loaded.index().postings("cat");
        EXPECT_EQ(cat_postings.size(), 2u);

        // "dog" appears in 1 document.
        const auto dog_postings = loaded.index().postings("dog");
        EXPECT_EQ(dog_postings.size(), 1u);
    }
}

TEST_F(ShardPersistenceTest, LoadMissingFileReturnsFalse)
{
    Shard shard("nonexistent_file_12345.jsonl");
    EXPECT_FALSE(shard.load());
}

TEST_F(ShardPersistenceTest, EmptyPathNoPersistence)
{
    Shard shard;  // no persistence path
    EXPECT_TRUE(shard.save());   // no-op
    EXPECT_FALSE(shard.load());  // no-op
}

// =========================================================================
// 7. Concurrency
// =========================================================================

TEST(ShardTest, ConcurrentAddDifferentIDs)
{
    Shard shard;
    constexpr int kThreads = 8;
    constexpr int kDocsPerThread = 100;

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&shard, t]() {
            for (int i = 0; i < kDocsPerThread; ++i) {
                const auto id = static_cast<doc_id>(t * kDocsPerThread + i);
                shard.add_document(id, "content " + std::to_string(id));
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    EXPECT_EQ(shard.document_count(),
              static_cast<std::size_t>(kThreads * kDocsPerThread));
}

TEST(ShardTest, ConcurrentReadsAndWrites)
{
    Shard shard;
    // Pre-populate some documents.
    for (doc_id i = 0; i < 100; ++i) {
        shard.add_document(i, "doc " + std::to_string(i));
    }

    std::vector<std::thread> threads;

    // Writer threads.
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&shard, t]() {
            for (int i = 0; i < 50; ++i) {
                const auto id = static_cast<doc_id>(1000 + t * 50 + i);
                shard.add_document(id, "new doc " + std::to_string(id));
            }
        });
    }

    // Reader threads.
    for (int t = 0; t < 4; ++t) {
        threads.emplace_back([&shard, t]() {
            for (doc_id i = 0; i < 100; ++i) {
                shard.contains_document(i);
                shard.get_document(i);
                shard.document_count();
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // All original + new documents should be present.
    EXPECT_GE(shard.document_count(), 100u);
}

// =========================================================================
// 8. Posting list sorted invariant after operations
// =========================================================================

TEST(ShardTest, PostingListsRemainSortedAfterUpdate)
{
    Shard shard;
    shard.add_document(1, "alpha");
    shard.add_document(2, "alpha");
    shard.add_document(3, "alpha");

    const auto postings = shard.index().postings("alpha");
    ASSERT_EQ(postings.size(), 3u);
    EXPECT_LE(postings[0].document_id, postings[1].document_id);
    EXPECT_LE(postings[1].document_id, postings[2].document_id);

    // Update doc 2 to remove "alpha".
    shard.update_document(2, "beta gamma");

    const auto updated = shard.index().postings("alpha");
    ASSERT_EQ(updated.size(), 2u);
    EXPECT_LE(updated[0].document_id, updated[1].document_id);
}

TEST(ShardTest, PostingListsRemainSortedAfterDelete)
{
    Shard shard;
    shard.add_document(1, "term");
    shard.add_document(2, "term");
    shard.add_document(3, "term");

    shard.remove_document(2);

    const auto postings = shard.index().postings("term");
    ASSERT_EQ(postings.size(), 2u);
    EXPECT_LE(postings[0].document_id, postings[1].document_id);
}

} // namespace
} // namespace dse
