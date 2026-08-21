// Distributed Search Engine - InvertedIndex Concurrency Tests (Phase 8A-2).
//
// Focused concurrency tests for dse::InvertedIndex thread safety.
// Verifies that concurrent reads and writes to the index are safe and
// produce consistent results.

#include <gtest/gtest.h>

#include "inverted_index.h"

#include <atomic>
#include <thread>
#include <vector>

namespace {

using dse::InvertedIndex;
using dse::Posting;
using dse::doc_id;

// Helper: create a simple index with a few documents.
InvertedIndex make_index()
{
    InvertedIndex index;
    index.add_document(1, "cat");
    index.add_document(2, "dog");
    index.add_document(3, "cat bird");
    return index;
}

} // namespace

// ---------------------------------------------------------------------------
// 1. Concurrent read-only postings()
// ---------------------------------------------------------------------------

TEST(InvertedIndexConcurrency, ConcurrentReadonlyPostings)
{
    InvertedIndex index = make_index();

    constexpr int kThreads = 8;
    constexpr int kIterations = 1000;

    std::atomic<int> success_count{0};

    auto reader = [&]() {
        for (int i = 0; i < kIterations; ++i) {
            const auto postings = index.postings("cat");
            if (postings.size() == 2u &&
                postings[0].document_id == 1 &&
                postings[1].document_id == 3) {
                ++success_count;
            }
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back(reader);
    }
    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count.load(), kThreads * kIterations);
}

// ---------------------------------------------------------------------------
// 2. Concurrent contains()
// ---------------------------------------------------------------------------

TEST(InvertedIndexConcurrency, ConcurrentContains)
{
    InvertedIndex index = make_index();

    constexpr int kThreads = 8;
    constexpr int kIterations = 1000;

    std::atomic<int> success_count{0};

    auto reader = [&]() {
        for (int i = 0; i < kIterations; ++i) {
            if (index.contains("cat") && !index.contains("nonexistent")) {
                ++success_count;
            }
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back(reader);
    }
    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count.load(), kThreads * kIterations);
}

// ---------------------------------------------------------------------------
// 3. Concurrent document_count() / term_count()
// ---------------------------------------------------------------------------

TEST(InvertedIndexConcurrency, ConcurrentCounts)
{
    InvertedIndex index = make_index();

    constexpr int kThreads = 8;
    constexpr int kIterations = 1000;

    std::atomic<int> success_count{0};

    auto reader = [&]() {
        for (int i = 0; i < kIterations; ++i) {
            if (index.document_count() == 3u && index.term_count() == 3u) {
                ++success_count;
            }
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back(reader);
    }
    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(success_count.load(), kThreads * kIterations);
}

// ---------------------------------------------------------------------------
// 4. Concurrent unique add_document()
// ---------------------------------------------------------------------------

TEST(InvertedIndexConcurrency, ConcurrentUniqueAdds)
{
    InvertedIndex index;

    constexpr int kThreads = 8;
    constexpr int kDocsPerThread = 100;

    std::atomic<int> add_successes{0};

    auto adder = [&](int thread_id) {
        for (int i = 0; i < kDocsPerThread; ++i) {
            doc_id id = static_cast<doc_id>(thread_id * kDocsPerThread + i + 1);
            // Each thread adds unique IDs, so no duplicates.
            index.add_document(id, "hello world");
            ++add_successes;
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back(adder, i);
    }
    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(add_successes.load(), kThreads * kDocsPerThread);
    EXPECT_EQ(index.document_count(), static_cast<std::size_t>(kThreads * kDocsPerThread));
}

// ---------------------------------------------------------------------------
// 5. Concurrent read + write
// ---------------------------------------------------------------------------

TEST(InvertedIndexConcurrency, ReadersDuringAdd)
{
    InvertedIndex index;
    index.add_document(1, "cat");

    constexpr int kReaders = 4;
    constexpr int kWriters = 2;
    constexpr int kWritesPerWriter = 50;
    constexpr int kReadsPerReader = 500;

    std::atomic<bool> done_writing{false};
    std::atomic<int> read_successes{0};

    auto reader = [&]() {
        for (int i = 0; i < kReadsPerReader; ++i) {
            // Reading should never crash or throw.
            const auto postings = index.postings("cat");
            (void)postings;
            index.contains("cat");
            index.document_count();
            index.term_count();
            ++read_successes;
        }
    };

    auto writer = [&](int start_id) {
        for (int i = 0; i < kWritesPerWriter; ++i) {
            doc_id id = static_cast<doc_id>(start_id + i);
            index.add_document(id, "new content");
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < kReaders; ++i) {
        threads.emplace_back(reader);
    }
    for (int i = 0; i < kWriters; ++i) {
        threads.emplace_back(writer, 2 + i * kWritesPerWriter);
    }
    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(read_successes.load(), kReaders * kReadsPerReader);
    EXPECT_EQ(index.document_count(),
              static_cast<std::size_t>(1 + kWriters * kWritesPerWriter));
}

// ---------------------------------------------------------------------------
// 6. Posting-list sorted invariant under concurrent adds
// ---------------------------------------------------------------------------

TEST(InvertedIndexConcurrency, PostingListsSortedAfterConcurrentAdds)
{
    InvertedIndex index;

    constexpr int kThreads = 8;
    constexpr int kDocsPerThread = 100;

    std::atomic<int> add_successes{0};

    auto adder = [&](int thread_id) {
        for (int i = 0; i < kDocsPerThread; ++i) {
            doc_id id = static_cast<doc_id>(thread_id * kDocsPerThread + i + 1);
            index.add_document(id, "common");
            ++add_successes;
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back(adder, i);
    }
    for (auto& t : threads) {
        t.join();
    }

    // After all concurrent adds, the postings list must be sorted.
    const auto postings = index.postings("common");
    ASSERT_EQ(postings.size(), static_cast<std::size_t>(kThreads * kDocsPerThread));
    for (std::size_t i = 1; i < postings.size(); ++i) {
        EXPECT_LT(postings[i - 1].document_id, postings[i].document_id)
            << "Postings not sorted at index " << i;
    }
}

// ---------------------------------------------------------------------------
// 7. Term-frequency correctness under concurrent adds
// ---------------------------------------------------------------------------

TEST(InvertedIndexConcurrency, TermFrequencyCorrectAfterConcurrentAdds)
{
    InvertedIndex index;

    constexpr int kDocs = 100;

    // Each doc has the same text, so TF("cat") = 2, TF("dog") = 1 for each.
    std::atomic<int> add_successes{0};

    auto adder = [&](int start_id) {
        for (int i = 0; i < kDocs; ++i) {
            doc_id id = static_cast<doc_id>(start_id + i);
            index.add_document(id, "cat cat dog");
            ++add_successes;
        }
    };

    std::vector<std::thread> threads;
    threads.emplace_back(adder, 1);
    threads.emplace_back(adder, kDocs + 1);
    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(add_successes.load(), 2 * kDocs);
    EXPECT_EQ(index.document_count(), static_cast<std::size_t>(2 * kDocs));

    const auto cat_postings = index.postings("cat");
    const auto dog_postings = index.postings("dog");

    ASSERT_EQ(cat_postings.size(), static_cast<std::size_t>(2 * kDocs));
    ASSERT_EQ(dog_postings.size(), static_cast<std::size_t>(2 * kDocs));

    for (const auto& p : cat_postings) {
        EXPECT_EQ(p.term_frequency, 2u);
    }
    for (const auto& p : dog_postings) {
        EXPECT_EQ(p.term_frequency, 1u);
    }
}

// ---------------------------------------------------------------------------
// 8. Document count correctness under concurrent adds
// ---------------------------------------------------------------------------

TEST(InvertedIndexConcurrency, DocumentCountCorrectAfterConcurrentAdds)
{
    InvertedIndex index;

    constexpr int kThreads = 4;
    constexpr int kDocsPerThread = 250;

    std::atomic<int> add_successes{0};

    auto adder = [&](int thread_id) {
        for (int i = 0; i < kDocsPerThread; ++i) {
            doc_id id = static_cast<doc_id>(thread_id * kDocsPerThread + i + 1);
            index.add_document(id, "test document");
            ++add_successes;
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < kThreads; ++i) {
        threads.emplace_back(adder, i);
    }
    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(add_successes.load(), kThreads * kDocsPerThread);
    EXPECT_EQ(index.document_count(),
              static_cast<std::size_t>(kThreads * kDocsPerThread));
    EXPECT_EQ(index.term_count(), 2u);  // "test" and "document"
}

// ---------------------------------------------------------------------------
// 9. Snapshot safety: returned vector survives concurrent modification
// ---------------------------------------------------------------------------

TEST(InvertedIndexConcurrency, SnapshotSurvivesConcurrentModification)
{
    InvertedIndex index;
    index.add_document(1, "cat");

    constexpr int kReaders = 4;
    constexpr int kWriters = 4;
    constexpr int kWritesPerWriter = 50;
    constexpr int kReadsPerReader = 500;

    std::atomic<int> read_successes{0};

    auto reader = [&]() {
        for (int i = 0; i < kReadsPerReader; ++i) {
            // Take a snapshot and verify it remains valid.
            const auto snapshot = index.postings("cat");
            // The snapshot should have exactly 1 entry (doc_id=1, tf=1)
            // regardless of what writers are doing.
            if (snapshot.size() == 1u && snapshot[0].document_id == 1) {
                ++read_successes;
            }
        }
    };

    auto writer = [&](int start_id) {
        for (int i = 0; i < kWritesPerWriter; ++i) {
            doc_id id = static_cast<doc_id>(start_id + i);
            index.add_document(id, "dog");
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < kReaders; ++i) {
        threads.emplace_back(reader);
    }
    for (int i = 0; i < kWriters; ++i) {
        threads.emplace_back(writer, 2 + i * kWritesPerWriter);
    }
    for (auto& t : threads) {
        t.join();
    }

    // All reads should have seen the original snapshot (doc_id=1).
    // Writers added new documents with term "dog", not "cat".
    EXPECT_EQ(read_successes.load(), kReaders * kReadsPerReader);
}

// ---------------------------------------------------------------------------
// 10. Mixed workload: readers + writers simultaneously
// ---------------------------------------------------------------------------

TEST(InvertedIndexConcurrency, MixedWorkload)
{
    InvertedIndex index;
    index.add_document(1, "cat");

    constexpr int kReaders = 4;
    constexpr int kWriters = 4;
    constexpr int kOpsPerThread = 200;

    std::atomic<bool> stop{false};
    std::atomic<int> read_ops{0};
    std::atomic<int> write_ops{0};

    auto reader = [&]() {
        while (!stop.load(std::memory_order_relaxed)) {
            // Random-ish read pattern
            index.postings("cat");
            index.postings("dog");
            index.contains("cat");
            index.document_count();
            index.term_count();
            ++read_ops;
        }
    };

    auto writer = [&](int start_id) {
        for (int i = 0; i < kOpsPerThread; ++i) {
            doc_id id = static_cast<doc_id>(start_id + i);
            index.add_document(id, "mixed content");
            ++write_ops;
        }
    };

    std::vector<std::thread> threads;
    for (int i = 0; i < kReaders; ++i) {
        threads.emplace_back(reader);
    }
    for (int i = 0; i < kWriters; ++i) {
        threads.emplace_back(writer, 2 + i * kOpsPerThread);
    }

    // Let writers finish, then stop readers.
    for (int i = kReaders; i < kReaders + kWriters; ++i) {
        threads[i].join();
    }
    stop.store(true, std::memory_order_relaxed);
    for (int i = 0; i < kReaders; ++i) {
        threads[i].join();
    }

    EXPECT_GT(read_ops.load(), 0);
    EXPECT_EQ(write_ops.load(), kWriters * kOpsPerThread);
    EXPECT_EQ(index.document_count(),
              static_cast<std::size_t>(1 + kWriters * kOpsPerThread));
}
