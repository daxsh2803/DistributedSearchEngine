// Distributed Search Engine - Retry Policy Tests (Phase 14).
//
// Tests the RetryPolicy abstraction: delay computation, error
// classification, and policy configuration.

#include <gtest/gtest.h>

#include <cstddef>
#include <string>

#include "retry_policy.h"

using namespace dse;

// ===========================================================================
// Delay computation
// ===========================================================================

TEST(RetryPolicyTest, FirstAttemptHasZeroDelay)
{
    RetryPolicy policy;
    policy.initial_delay_ms = 100;
    policy.backoff_multiplier = 2.0;
    EXPECT_EQ(policy.delay_for_attempt(0), 0u);
}

TEST(RetryPolicyTest, ExponentialBackoff)
{
    RetryPolicy policy;
    policy.initial_delay_ms = 100;
    policy.backoff_multiplier = 2.0;
    policy.max_delay_ms = 10000;

    EXPECT_EQ(policy.delay_for_attempt(0), 0u);    // First attempt: no delay
    EXPECT_EQ(policy.delay_for_attempt(1), 100u);   // 100 * 2^0 = 100
    EXPECT_EQ(policy.delay_for_attempt(2), 200u);   // 100 * 2^1 = 200
    EXPECT_EQ(policy.delay_for_attempt(3), 400u);   // 100 * 2^2 = 400
    EXPECT_EQ(policy.delay_for_attempt(4), 800u);   // 100 * 2^3 = 800
}

TEST(RetryPolicyTest, DelayRespectsMaxCap)
{
    RetryPolicy policy;
    policy.initial_delay_ms = 100;
    policy.backoff_multiplier = 2.0;
    policy.max_delay_ms = 500;

    EXPECT_EQ(policy.delay_for_attempt(0), 0u);
    EXPECT_EQ(policy.delay_for_attempt(1), 100u);
    EXPECT_EQ(policy.delay_for_attempt(2), 200u);
    EXPECT_EQ(policy.delay_for_attempt(3), 400u);
    EXPECT_EQ(policy.delay_for_attempt(4), 500u);  // Capped at 500
    EXPECT_EQ(policy.delay_for_attempt(5), 500u);  // Still capped
}

TEST(RetryPolicyTest, DifferentBackoffMultiplier)
{
    RetryPolicy policy;
    policy.initial_delay_ms = 50;
    policy.backoff_multiplier = 3.0;
    policy.max_delay_ms = 10000;

    EXPECT_EQ(policy.delay_for_attempt(0), 0u);
    EXPECT_EQ(policy.delay_for_attempt(1), 50u);    // 50 * 3^0 = 50
    EXPECT_EQ(policy.delay_for_attempt(2), 150u);   // 50 * 3^1 = 150
    EXPECT_EQ(policy.delay_for_attempt(3), 450u);   // 50 * 3^2 = 450
}

// ===========================================================================
// Error classification
// ===========================================================================

TEST(RetryPolicyTest, ClassifyConnectionRefused)
{
    auto cat = RetryPolicy::classify_error("Connection failed to node 0");
    EXPECT_EQ(cat, ErrorCategory::ConnectionRefused);
}

TEST(RetryPolicyTest, ClassifyConnectionReset)
{
    auto cat = RetryPolicy::classify_error("Connection reset by peer");
    EXPECT_EQ(cat, ErrorCategory::ConnectionReset);
}

TEST(RetryPolicyTest, ClassifyTimeout)
{
    auto cat = RetryPolicy::classify_error("Read timeout");
    EXPECT_EQ(cat, ErrorCategory::Timeout);
}

TEST(RetryPolicyTest, ClassifyTransportError)
{
    auto cat = RetryPolicy::classify_error("Request failed: some exception");
    EXPECT_EQ(cat, ErrorCategory::TransportError);
}

TEST(RetryPolicyTest, ClassifyDocumentNotFound)
{
    auto cat = RetryPolicy::classify_error("Document not found");
    EXPECT_EQ(cat, ErrorCategory::DocumentNotFound);
}

TEST(RetryPolicyTest, ClassifyDocumentAlreadyExists)
{
    auto cat = RetryPolicy::classify_error("Document already exists");
    EXPECT_EQ(cat, ErrorCategory::DocumentAlreadyExists);
}

TEST(RetryPolicyTest, ClassifyInvalidRequest)
{
    auto cat = RetryPolicy::classify_error("Invalid request: missing field");
    EXPECT_EQ(cat, ErrorCategory::InvalidRequest);
}

TEST(RetryPolicyTest, ClassifyServerError)
{
    auto cat = RetryPolicy::classify_error("Internal server error");
    EXPECT_EQ(cat, ErrorCategory::ServerError);
}

TEST(RetryPolicyTest, ClassifyUnknown)
{
    auto cat = RetryPolicy::classify_error("Something unexpected happened");
    EXPECT_EQ(cat, ErrorCategory::Unknown);
}

// ===========================================================================
// Retryable classification
// ===========================================================================

TEST(RetryPolicyTest, TransientErrorsAreRetryable)
{
    RetryPolicy policy;
    EXPECT_TRUE(policy.is_retryable(ErrorCategory::ConnectionRefused));
    EXPECT_TRUE(policy.is_retryable(ErrorCategory::ConnectionReset));
    EXPECT_TRUE(policy.is_retryable(ErrorCategory::Timeout));
    EXPECT_TRUE(policy.is_retryable(ErrorCategory::TransportError));
}

TEST(RetryPolicyTest, ApplicationErrorsAreNotRetryable)
{
    RetryPolicy policy;
    EXPECT_FALSE(policy.is_retryable(ErrorCategory::InvalidRequest));
    EXPECT_FALSE(policy.is_retryable(ErrorCategory::DocumentNotFound));
    EXPECT_FALSE(policy.is_retryable(ErrorCategory::DocumentAlreadyExists));
    EXPECT_FALSE(policy.is_retryable(ErrorCategory::ServerError));
    EXPECT_FALSE(policy.is_retryable(ErrorCategory::Unknown));
}

// ===========================================================================
// Policy factory methods
// ===========================================================================

TEST(RetryPolicyTest, NoRetriesPolicy)
{
    auto policy = RetryPolicy::no_retries();
    EXPECT_EQ(policy.max_attempts, 1u);
    EXPECT_EQ(policy.delay_for_attempt(0), 0u);
}

TEST(RetryPolicyTest, DefaultPolicy)
{
    auto policy = RetryPolicy::default_policy();
    EXPECT_EQ(policy.max_attempts, 3u);
    EXPECT_EQ(policy.initial_delay_ms, 100u);
    EXPECT_EQ(policy.max_delay_ms, 1000u);
    EXPECT_DOUBLE_EQ(policy.backoff_multiplier, 2.0);
    EXPECT_FALSE(policy.retry_writes);
}
