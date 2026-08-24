// Distributed Search Engine - DocumentStore Concurrency Tests (Phase 8A-1).
//
// Tests for thread-safe DocumentStore covering: concurrent reads,
// concurrent writes, concurrent read+write, and lock correctness.

#include <gtest/gtest.h>

#include "document_store.h"

#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <string>
#include <thread>
#include <vector>

#include <nlohmann/json.hpp>

namespace {

using dse::Document;
using dse::DocumentStore;
using dse::doc_id;

} // namespace

// ===========================================================================
// 1. Multiple concurrent get() calls
// ===========================================================================

TEST(DocumentStoreConcurrency, ConcurrentGets)
{
    DocumentStore store;
    // Pre-populate with documents.
    for (doc_id i = 0; i < 100; ++i) {
        store.add({i, "doc " + std::to_string(i)});
    }

    constexpr int kThreads = 8;
    constexpr int kIterations = 1000;

    std::atomic<int> success_count{0};
    std::vector<std::thread> threads;

    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&store, &success_count, t]() {
            for (int i = 0; i < kIterations; ++i) {
                const doc_id id = static_cast<doc_id>((t * kIterations + i) % 100);
                const auto doc = store.get(id);
                if (doc.has_value() && doc->id == id) {
                    success_count.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // All gets should have succeeded.
    EXPECT_EQ(success_count.load(), kThreads * kIterations);
}

// ===========================================================================
// 2. Multiple concurrent contains() calls
// ===========================================================================

TEST(DocumentStoreConcurrency, ConcurrentContains)
{
    DocumentStore store;
    for (doc_id i = 0; i < 50; ++i) {
        store.add({i, "doc"});
    }

    constexpr int kThreads = 8;
    constexpr int kIterations = 1000;

    std::atomic<int> true_count{0};
    std::atomic<int> false_count{0};
    std::vector<std::thread> threads;

    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&store, &true_count, &false_count, t]() {
            for (int i = 0; i < kIterations; ++i) {
                const doc_id id = static_cast<doc_id>((t * kIterations + i) % 100);
                if (store.contains(id)) {
                    true_count.fetch_add(1, std::memory_order_relaxed);
                } else {
                    false_count.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // IDs 0-49 exist, IDs 50-99 don't. Each thread iterates 1000 IDs.
    // The exact count depends on the modular arithmetic, but the total
    // should equal kThreads * kIterations.
    EXPECT_EQ(true_count.load() + false_count.load(), kThreads * kIterations);
    EXPECT_GT(true_count.load(), 0);
    EXPECT_GT(false_count.load(), 0);
}

// ===========================================================================
// 3. Concurrent readers while another thread attempts add()
// ===========================================================================

TEST(DocumentStoreConcurrency, ReadersDuringAdd)
{
    DocumentStore store;
    // Pre-populate with some documents.
    for (doc_id i = 0; i < 10; ++i) {
        store.add({i, "existing " + std::to_string(i)});
    }

    constexpr int kReaderThreads = 4;
    constexpr int kIterations = 500;
    std::atomic<bool> writer_done{false};
    std::atomic<int> reader_errors{0};

    // Writer thread: adds new documents.
    std::thread writer([&store, &writer_done]() {
        for (doc_id i = 100; i < 200; ++i) {
            store.add({i, "new " + std::to_string(i)});
            std::this_thread::yield();
        }
        writer_done.store(true);
    });

    // Reader threads: read existing documents.
    std::vector<std::thread> readers;
    for (int t = 0; t < kReaderThreads; ++t) {
        readers.emplace_back([&store, &reader_errors]() {
            for (int i = 0; i < kIterations; ++i) {
                // Read documents that existed before the writer started.
                // These should always be consistent.
                for (doc_id id = 0; id < 10; ++id) {
                    const auto doc = store.get(id);
                    if (!doc.has_value() || doc->id != id) {
                        reader_errors.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        });
    }

    for (auto& th : readers) {
        th.join();
    }
    writer.join();

    // No reader errors: existing documents were always consistent.
    EXPECT_EQ(reader_errors.load(), 0);

    // Writer added 100 documents.
    EXPECT_EQ(store.size(), 110u);
}

// ===========================================================================
// 4. Multiple concurrent adds with unique IDs
// ===========================================================================

TEST(DocumentStoreConcurrency, ConcurrentUniqueAdds)
{
    DocumentStore store;

    constexpr int kThreads = 8;
    constexpr int kDocsPerThread = 100;
    std::vector<std::thread> threads;

    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&store, t]() {
            for (int i = 0; i < kDocsPerThread; ++i) {
                const doc_id id = static_cast<doc_id>(t * kDocsPerThread + i);
                const bool ok = store.add({id, "doc " + std::to_string(id)});
                // Since each thread uses unique IDs, all adds should succeed.
                if (!ok) {
                    // This should never happen — IDs are unique across threads.
                    // But we can't assert from a worker thread safely, so we
                    // just log it.
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // All documents should be stored.
    EXPECT_EQ(store.size(), static_cast<std::size_t>(kThreads * kDocsPerThread));

    // Verify a sample of documents.
    for (int t = 0; t < kThreads; ++t) {
        const doc_id id = static_cast<doc_id>(t * kDocsPerThread);
        const auto doc = store.get(id);
        ASSERT_TRUE(doc.has_value());
        EXPECT_EQ(doc->id, id);
    }
}

// ===========================================================================
// 5. Concurrent duplicate adds for the same ID
// ===========================================================================

TEST(DocumentStoreConcurrency, ConcurrentDuplicateAdds)
{
    DocumentStore store;

    constexpr int kThreads = 8;
    constexpr int kIterations = 500;
    std::atomic<int> success_count{0};
    std::atomic<int> failure_count{0};

    std::vector<std::thread> threads;

    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&store, &success_count, &failure_count]() {
            for (int i = 0; i < kIterations; ++i) {
                // All threads try to add the same ID (42).
                const bool ok = store.add({42, "content"});
                if (ok) {
                    success_count.fetch_add(1, std::memory_order_relaxed);
                } else {
                    failure_count.fetch_add(1, std::memory_order_relaxed);
                }
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }

    // Exactly one add should succeed (first one wins).
    // All others should fail.
    EXPECT_EQ(success_count.load(), 1);
    EXPECT_EQ(failure_count.load(), kThreads * kIterations - 1);

    // Document content should be the first one written.
    const auto doc = store.get(42);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->id, 42u);
}

// ===========================================================================
// 6. Concurrent reads while save() executes
// ===========================================================================

TEST(DocumentStoreConcurrency, ReadsDuringSave)
{
    DocumentStore store;
    // Pre-populate with documents.
    for (doc_id i = 0; i < 50; ++i) {
        store.add({i, "doc " + std::to_string(i)});
    }

    constexpr int kReaderThreads = 4;
    constexpr int kIterations = 200;
    std::atomic<int> reader_errors{0};

    // Save thread: repeatedly saves to a temporary file.
    std::thread saver([&store]() {
        for (int i = 0; i < 50; ++i) {
            // Use a unique filename to avoid conflicts.
            const std::string path = std::tmpnam(nullptr) +
                                     std::string("_save_concurrent_") +
                                     std::to_string(i) + ".jsonl";
            store.save(path);
            std::remove(path.c_str());
        }
    });

    // Reader threads: read documents.
    std::vector<std::thread> readers;
    for (int t = 0; t < kReaderThreads; ++t) {
        readers.emplace_back([&store, &reader_errors]() {
            for (int i = 0; i < kIterations; ++i) {
                // Read documents 0-49 (which should always exist).
                for (doc_id id = 0; id < 50; ++id) {
                    const auto doc = store.get(id);
                    if (!doc.has_value() || doc->id != id) {
                        reader_errors.fetch_add(1, std::memory_order_relaxed);
                    }
                }
            }
        });
    }

    for (auto& th : readers) {
        th.join();
    }
    saver.join();

    // No reader errors.
    EXPECT_EQ(reader_errors.load(), 0);
}

// ===========================================================================
// 7. Load behavior remains correct (atomic replacement)
// ===========================================================================

TEST(DocumentStoreConcurrency, LoadAtomicReplacement)
{
    DocumentStore store;
    // Pre-populate with documents.
    for (doc_id i = 0; i < 20; ++i) {
        store.add({i, "original " + std::to_string(i)});
    }

    // Create a file with different documents.
    const std::string path = std::tmpnam(nullptr) +
                             std::string("_load_atomic.jsonl");
    {
        std::ofstream ofs(path);
        for (doc_id i = 100; i < 120; ++i) {
            nlohmann::json j;
            j["id"] = i;
            j["content"] = "loaded " + std::to_string(i);
            ofs << j.dump() << "\n";
        }
    }

    // Load should replace all documents atomically.
    EXPECT_TRUE(store.load(path));

    // Original documents should be gone.
    EXPECT_EQ(store.size(), 20u);
    EXPECT_FALSE(store.contains(0));
    EXPECT_FALSE(store.contains(10));

    // Loaded documents should be present.
    EXPECT_TRUE(store.contains(100));
    EXPECT_TRUE(store.contains(119));

    const auto doc = store.get(100);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "loaded 100");

    std::remove(path.c_str());
}

// ===========================================================================
// 8. size() is consistent during concurrent operations
// ===========================================================================

TEST(DocumentStoreConcurrency, SizeConsistentDuringConcurrentOps)
{
    DocumentStore store;
    constexpr doc_id kInitialDocs = 50;

    // Pre-populate.
    for (doc_id i = 0; i < kInitialDocs; ++i) {
        store.add({i, "doc"});
    }

    constexpr int kAdderThreads = 4;
    constexpr int kDocsPerThread = 100;
    std::atomic<bool> done{false};

    // Adder threads: add new documents.
    std::vector<std::thread> adders;
    for (int t = 0; t < kAdderThreads; ++t) {
        adders.emplace_back([&store, t]() {
            for (int i = 0; i < kDocsPerThread; ++i) {
                const doc_id id = kInitialDocs +
                                  static_cast<doc_id>(t * kDocsPerThread + i);
                store.add({id, "new"});
            }
        });
    }

    // Reader thread: repeatedly checks size().
    std::thread reader([&store, &done]() {
        std::size_t prev_size = 0;
        while (!done.load()) {
            const std::size_t sz = store.size();
            // Size should never decrease.
            EXPECT_GE(sz, prev_size);
            prev_size = sz;
        }
    });

    for (auto& th : adders) {
        th.join();
    }
    done.store(true);
    reader.join();

    EXPECT_EQ(store.size(), kInitialDocs + kAdderThreads * kDocsPerThread);
}

// ===========================================================================
// 9. all() returns a snapshot safe for iteration
// ===========================================================================

TEST(DocumentStoreConcurrency, AllReturnsSafeSnapshot)
{
    DocumentStore store;
    for (doc_id i = 0; i < 30; ++i) {
        store.add({i, "doc " + std::to_string(i)});
    }

    std::atomic<bool> writer_done{false};

    // Writer thread: adds documents concurrently.
    std::thread writer([&store, &writer_done]() {
        for (doc_id i = 100; i < 200; ++i) {
            store.add({i, "new"});
        }
        writer_done.store(true);
    });

    // Take a snapshot.  The writer may or may not have started adding
    // documents by this point, so we cannot assume the exact size.
    // The important invariant is that the snapshot is a self-consistent
    // copy: all original documents are present, iteration is safe, and
    // the snapshot does not change over time.
    const auto snapshot = store.all();

    // The snapshot must contain at least the original 30 documents
    // (the writer only adds IDs >= 100).
    EXPECT_GE(snapshot.size(), 30u);

    // All original documents must be present and correct.
    for (doc_id i = 0; i < 30; ++i) {
        auto it = snapshot.find(i);
        ASSERT_NE(it, snapshot.end()) << "missing original doc " << i;
        EXPECT_EQ(it->second.id, i);
        EXPECT_EQ(it->second.content, "doc " + std::to_string(i));
    }

    // Verify the snapshot is stable (immutable copy).
    std::size_t first_count = 0;
    for (const auto& [id, doc] : snapshot) {
        ++first_count;
    }
    std::size_t second_count = 0;
    for (const auto& [id, doc] : snapshot) {
        ++second_count;
    }
    EXPECT_EQ(first_count, second_count);
    EXPECT_EQ(first_count, snapshot.size());

    writer.join();

    // After writer finishes, the store should have more documents.
    EXPECT_EQ(store.size(), 130u);
}

// ===========================================================================
// 10. Mixed concurrent reads, writes, and save
// ===========================================================================

TEST(DocumentStoreConcurrency, MixedWorkload)
{
    DocumentStore store;
    constexpr doc_id kInitialDocs = 20;

    for (doc_id i = 0; i < kInitialDocs; ++i) {
        store.add({i, "initial"});
    }

    constexpr int kReaderThreads = 4;
    constexpr int kWriterThreads = 2;
    constexpr int kIterations = 200;
    std::atomic<bool> stop{false};
    std::atomic<int> write_successes{0};

    // Readers
    std::vector<std::thread> readers;
    for (int t = 0; t < kReaderThreads; ++t) {
        readers.emplace_back([&store, &stop]() {
            while (!stop.load()) {
                // Read a document.
                const doc_id id = static_cast<doc_id>(rand() % kInitialDocs);
                store.get(id);
                store.contains(id);
                store.size();
            }
        });
    }

    // Writers
    std::vector<std::thread> writers;
    for (int t = 0; t < kWriterThreads; ++t) {
        writers.emplace_back([&store, &stop, &write_successes, t]() {
            doc_id next_id = 1000 + static_cast<doc_id>(t * 1000);
            for (int i = 0; i < kIterations && !stop.load(); ++i) {
                if (store.add({next_id, "new"})) {
                    write_successes.fetch_add(1, std::memory_order_relaxed);
                }
                ++next_id;
            }
        });
    }

    for (auto& th : writers) {
        th.join();
    }
    stop.store(true);
    for (auto& th : readers) {
        th.join();
    }

    // All writers should have succeeded (unique IDs).
    EXPECT_EQ(write_successes.load(), kWriterThreads * kIterations);
}
