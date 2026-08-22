// Distributed Search Engine - Shard Router Tests (Phase 10A).
//
// Tests for dse::ShardRouter: deterministic routing, distribution,
// edge cases, and thread safety.

#include "shard_router.h"

#include <gtest/gtest.h>

#include <cstddef>
#include <cstdint>
#include <set>
#include <thread>
#include <vector>

namespace dse {
namespace {

// =========================================================================
// 1. Basic construction and routing
// =========================================================================

TEST(ShardRouterTest, SingleShardRoutesEverythingToOneShard)
{
    const ShardRouter router(1);
    EXPECT_EQ(router.shard_count(), 1u);
    EXPECT_EQ(router.route(0), 0u);
    EXPECT_EQ(router.route(1), 0u);
    EXPECT_EQ(router.route(42), 0u);
    EXPECT_EQ(router.route(1000000), 0u);
}

TEST(ShardRouterTest, RouteReturnsValidShardIndex)
{
    const ShardRouter router(3);
    for (doc_id id = 0; id < 1000; ++id) {
        const auto shard = router.route(id);
        EXPECT_LT(shard, 3u) << "doc_id " << id << " routed to invalid shard " << shard;
    }
}

TEST(ShardRouterTest, RouteIsDeterministic)
{
    const ShardRouter router(5);
    for (doc_id id = 0; id < 500; ++id) {
        const auto first = router.route(id);
        const auto second = router.route(id);
        const auto third = router.route(id);
        EXPECT_EQ(first, second);
        EXPECT_EQ(second, third);
    }
}

// =========================================================================
// 2. Distribution
// =========================================================================

TEST(ShardRouterTest, DistributesAcrossAllShards)
{
    const ShardRouter router(4);
    std::set<std::size_t> seen;
    for (doc_id id = 0; id < 10000; ++id) {
        seen.insert(router.route(id));
    }
    EXPECT_EQ(seen.size(), 4u) << "Not all shards received any documents";
}

TEST(ShardRouterTest, ReasonableDistributionBalance)
{
    const ShardRouter router(4);
    std::vector<std::size_t> counts(4, 0);
    constexpr doc_id N = 100000;
    for (doc_id id = 0; id < N; ++id) {
        ++counts[router.route(id)];
    }
    // Each shard should get at least 10% and at most 40% of documents.
    for (std::size_t i = 0; i < 4; ++i) {
        const double fraction = static_cast<double>(counts[i]) / N;
        EXPECT_GT(fraction, 0.10) << "Shard " << i << " too empty";
        EXPECT_LT(fraction, 0.40) << "Shard " << i << " too full";
    }
}

// =========================================================================
// 3. Edge cases
// =========================================================================

TEST(ShardRouterTest, ZeroShardCountThrows)
{
    EXPECT_THROW(ShardRouter(0), std::invalid_argument);
}

TEST(ShardRouterTest, LargeShardCount)
{
    const ShardRouter router(1000);
    EXPECT_EQ(router.shard_count(), 1000u);
    for (doc_id id = 0; id < 100; ++id) {
        EXPECT_LT(router.route(id), 1000u);
    }
}

TEST(ShardRouterTest, MaxDocId)
{
    const ShardRouter router(3);
    const doc_id max_id = UINT32_MAX;
    EXPECT_LT(router.route(max_id), 3u);
    // Deterministic even at max
    EXPECT_EQ(router.route(max_id), router.route(max_id));
}

// =========================================================================
// 4. Deterministic across constructions
// =========================================================================

TEST(ShardRouterTest, SameShardCountProducesSameRouting)
{
    // Two routers with the same shard_count should route identically.
    const ShardRouter a(7);
    const ShardRouter b(7);
    for (doc_id id = 0; id < 1000; ++id) {
        EXPECT_EQ(a.route(id), b.route(id));
    }
}

TEST(ShardRouterTest, DifferentShardCountProducesDifferentRouting)
{
    const ShardRouter a(3);
    const ShardRouter b(5);
    // At least some IDs should route differently.
    int different = 0;
    for (doc_id id = 0; id < 1000; ++id) {
        if (a.route(id) != b.route(id)) {
            ++different;
        }
    }
    EXPECT_GT(different, 0);
}

// =========================================================================
// 5. Thread safety
// =========================================================================

TEST(ShardRouterTest, ConcurrentRoutingIsSafe)
{
    const ShardRouter router(4);
    constexpr int kThreads = 8;
    constexpr doc_id kIdsPerThread = 10000;

    std::vector<std::thread> threads;
    for (int t = 0; t < kThreads; ++t) {
        threads.emplace_back([&router, t]() {
            for (doc_id id = 0; id < kIdsPerThread; ++id) {
                const auto shard = router.route(id + static_cast<doc_id>(t) * kIdsPerThread);
                EXPECT_LT(shard, router.shard_count());
            }
        });
    }

    for (auto& th : threads) {
        th.join();
    }
}

} // namespace
} // namespace dse
