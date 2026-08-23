// Distributed Search Engine - Retry Policy (Phase 14).
//
// Provides a configurable, testable retry policy for transient network
// failures. The policy is independent of HTTP/server code and defines:
//
//   - Maximum retry attempts (bounded)
//   - Retry delay with exponential backoff
//   - Which error categories are retryable
//
// Design principles:
//   - Immutable after construction (thread-safe by construction)
//   - Deterministic (no jitter, no randomness)
//   - Simple and testable
//   - Does NOT retry application-level errors

#pragma once

#include <cstddef>
#include <string>

namespace dse {

// Error classification for retry decisions.
enum class ErrorCategory {
    // Transient network failures — safe to retry.
    ConnectionRefused,
    ConnectionReset,
    Timeout,
    TransportError,

    // Application-level errors — should NOT be retried.
    InvalidRequest,
    DocumentNotFound,
    DocumentAlreadyExists,
    ServerError,
    Unknown
};

// Retry policy configuration.
//
// Thread safety:
//   Immutable after construction. All methods are const.
struct RetryPolicy {
    // Total number of attempts (1 = no retries, 3 = 2 retries after initial).
    std::size_t max_attempts = 3;

    // Initial delay before the first retry, in milliseconds.
    std::size_t initial_delay_ms = 100;

    // Maximum delay cap, in milliseconds.
    std::size_t max_delay_ms = 1000;

    // Backoff multiplier for exponential backoff.
    double backoff_multiplier = 2.0;

    // Whether to retry write operations (default: false for safety).
    // Writes are non-idempotent in the current API, so blindly retrying
    // can cause duplicate ingestion or side effects.
    bool retry_writes = false;

    // Compute the delay for a given retry attempt (0-indexed).
    // Returns 0 for the first attempt, then exponential backoff.
    std::size_t delay_for_attempt(std::size_t attempt) const;

    // Check if an error should be retried based on its category.
    bool is_retryable(ErrorCategory category) const;

    // Parse an error message to determine its category.
    // This is a simple heuristic based on common error patterns.
    static ErrorCategory classify_error(const std::string& error_message);

    // Create a no-retry policy (max_attempts = 1).
    static RetryPolicy no_retries();

    // Create a default policy with sensible defaults.
    static RetryPolicy default_policy();
};

} // namespace dse
