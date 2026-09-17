// Distributed Search Engine - Circuit Breaker Tests (Phase 15).
//
// Tests the CircuitBreaker state machine: CLOSED → OPEN → HALF_OPEN → CLOSED.
// Verifies thread safety, state transitions, and failure threshold behavior.

#include <gtest/gtest.h>

#include <chrono>
#include <cstddef>
#include <thread>
#include <vector>

#include "circuit_breaker.h"

using namespace dse;

// ===========================================================================
// Initial state
// ===========================================================================

TEST(CircuitBreakerTest, InitialStateIsClosed)
{
    CircuitBreaker cb;
    EXPECT_EQ(cb.state(), CircuitState::Closed);
}

TEST(CircuitBreakerTest, InitialConsecutiveFailuresIsZero)
{
    CircuitBreaker cb;
    EXPECT_EQ(cb.consecutive_failures(), 0u);
}

TEST(CircuitBreakerTest, ShouldAllowRequestWhenClosed)
{
    CircuitBreaker cb;
    EXPECT_TRUE(cb.should_allow_request());
}

// ===========================================================================
// CLOSED → OPEN transition
// ===========================================================================

TEST(CircuitBreakerTest, TripsAfterFailureThreshold)
{
    CircuitBreakerConfig config;
    config.failure_threshold = 3;
    CircuitBreaker cb(config);

    // First two failures — still CLOSED.
    cb.record_failure();
    EXPECT_EQ(cb.state(), CircuitState::Closed);
    EXPECT_EQ(cb.consecutive_failures(), 1u);

    cb.record_failure();
    EXPECT_EQ(cb.state(), CircuitState::Closed);
    EXPECT_EQ(cb.consecutive_failures(), 2u);

    // Third failure — trips to OPEN.
    cb.record_failure();
    EXPECT_EQ(cb.state(), CircuitState::Open);
}

TEST(CircuitBreakerTest, FailureCountResetsOnSuccess)
{
    CircuitBreakerConfig config;
    config.failure_threshold = 5;
    CircuitBreaker cb(config);

    cb.record_failure();
    cb.record_failure();
    cb.record_failure();
    EXPECT_EQ(cb.consecutive_failures(), 3u);

    // Success resets the count.
    cb.record_success();
    EXPECT_EQ(cb.consecutive_failures(), 0u);
    EXPECT_EQ(cb.state(), CircuitState::Closed);
}

// ===========================================================================
// OPEN state behavior
// ===========================================================================

TEST(CircuitBreakerTest, OpenStateFailsFast)
{
    CircuitBreakerConfig config;
    config.failure_threshold = 1;
    config.recovery_timeout_ms = 60000;  // Long timeout
    CircuitBreaker cb(config);

    cb.record_failure();  // Trip to OPEN
    EXPECT_EQ(cb.state(), CircuitState::Open);

    // Should fail fast.
    EXPECT_FALSE(cb.should_allow_request());
}

// ===========================================================================
// OPEN → HALF_OPEN transition
// ===========================================================================

TEST(CircuitBreakerTest, TransitionsToHalfOpenAfterRecoveryTimeout)
{
    CircuitBreakerConfig config;
    config.failure_threshold = 1;
    config.recovery_timeout_ms = 50;  // 50ms for testing
    CircuitBreaker cb(config);

    cb.record_failure();  // Trip to OPEN
    EXPECT_EQ(cb.state(), CircuitState::Open);

    // Wait for recovery timeout.
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    // Should now transition to HALF_OPEN and allow a probe.
    EXPECT_TRUE(cb.should_allow_request());
    EXPECT_EQ(cb.state(), CircuitState::HalfOpen);
}

// ===========================================================================
// HALF_OPEN behavior
// ===========================================================================

TEST(CircuitBreakerTest, HalfOpenSuccessClosesCircuit)
{
    CircuitBreakerConfig config;
    config.failure_threshold = 1;
    config.recovery_timeout_ms = 50;
    config.half_open_max_probes = 1;
    CircuitBreaker cb(config);

    cb.record_failure();  // Trip to OPEN
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    // Transition to HALF_OPEN.
    EXPECT_TRUE(cb.should_allow_request());
    EXPECT_EQ(cb.state(), CircuitState::HalfOpen);

    // Successful probe closes the circuit.
    cb.record_success();
    EXPECT_EQ(cb.state(), CircuitState::Closed);
    EXPECT_EQ(cb.consecutive_failures(), 0u);
}

TEST(CircuitBreakerTest, HalfOpenFailureReopensCircuit)
{
    CircuitBreakerConfig config;
    config.failure_threshold = 1;
    config.recovery_timeout_ms = 50;
    config.half_open_max_probes = 1;
    CircuitBreaker cb(config);

    cb.record_failure();  // Trip to OPEN
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    // Transition to HALF_OPEN.
    EXPECT_TRUE(cb.should_allow_request());

    // Failed probe reopens the circuit.
    cb.record_failure();
    EXPECT_EQ(cb.state(), CircuitState::Open);
}

TEST(CircuitBreakerTest, HalfOpenLimitsProbes)
{
    CircuitBreakerConfig config;
    config.failure_threshold = 1;
    config.recovery_timeout_ms = 50;
    config.half_open_max_probes = 2;
    CircuitBreaker cb(config);

    cb.record_failure();  // Trip to OPEN
    std::this_thread::sleep_for(std::chrono::milliseconds(60));

    // First probe allowed (transitions to HALF_OPEN).
    EXPECT_TRUE(cb.should_allow_request());
    // Second probe allowed (probe count decremented).
    EXPECT_TRUE(cb.should_allow_request());
    // Third probe denied (no probes remaining).
    EXPECT_FALSE(cb.should_allow_request());
}

// ===========================================================================
// Reset
// ===========================================================================

TEST(CircuitBreakerTest, ResetReturnsToInitialState)
{
    CircuitBreakerConfig config;
    config.failure_threshold = 1;
    CircuitBreaker cb(config);

    cb.record_failure();  // Trip to OPEN
    EXPECT_EQ(cb.state(), CircuitState::Open);

    cb.reset();
    EXPECT_EQ(cb.state(), CircuitState::Closed);
    EXPECT_EQ(cb.consecutive_failures(), 0u);
    EXPECT_TRUE(cb.should_allow_request());
}

// ===========================================================================
// Configuration
// ===========================================================================

TEST(CircuitBreakerTest, CustomConfiguration)
{
    CircuitBreakerConfig config;
    config.failure_threshold = 10;
    config.recovery_timeout_ms = 1000;
    config.half_open_max_probes = 3;
    CircuitBreaker cb(config);

    EXPECT_EQ(cb.config().failure_threshold, 10u);
    EXPECT_EQ(cb.config().recovery_timeout_ms, 1000u);
    EXPECT_EQ(cb.config().half_open_max_probes, 3u);
}

// ===========================================================================
// Thread safety
// ===========================================================================

TEST(CircuitBreakerTest, ConcurrentAccessIsSafe)
{
    CircuitBreakerConfig config;
    config.failure_threshold = 100;  // High threshold to avoid tripping
    CircuitBreaker cb(config);

    // Run multiple threads recording successes and failures.
    std::vector<std::thread> threads;
    for (int i = 0; i < 10; ++i) {
        threads.emplace_back([&cb, i]() {
            for (int j = 0; j < 100; ++j) {
                if ((i + j) % 3 == 0) {
                    cb.record_failure();
                } else {
                    cb.record_success();
                }
                cb.should_allow_request();
            }
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    // No crash or undefined behavior = pass.
    EXPECT_TRUE(true);
}

// ===========================================================================
// Default configuration
// ===========================================================================

TEST(CircuitBreakerTest, DefaultConfigValues)
{
    CircuitBreakerConfig config;
    EXPECT_EQ(config.failure_threshold, 5u);
    EXPECT_EQ(config.recovery_timeout_ms, 5000u);
    EXPECT_EQ(config.half_open_max_probes, 1u);
}
