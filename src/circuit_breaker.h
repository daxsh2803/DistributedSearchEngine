// Distributed Search Engine - Circuit Breaker (Phase 15).
//
// Prevents repeated requests to unhealthy remote nodes by tracking
// consecutive failures and temporarily blocking requests when the
// failure threshold is exceeded.
//
// State machine:
//   CLOSED → OPEN → HALF_OPEN → CLOSED
//
// CLOSED (normal):
//   - All requests allowed
//   - Consecutive failures tracked
//   - Threshold exceeded → transition to OPEN
//
// OPEN (blocking):
//   - All requests fail fast (no network I/O)
//   - After recovery timeout → transition to HALF_OPEN
//
// HALF_OPEN (probing):
//   - Limited probe requests allowed
//   - Successful probe → transition to CLOSED
//   - Failed probe → transition to OPEN
//
// Thread safety:
//   All methods are safe for concurrent use. Internal state is
//   protected by a mutex. The circuit breaker does not perform I/O,
//   so holding the lock during state transitions is safe.

#pragma once

#include <chrono>
#include <cstddef>
#include <memory>
#include <mutex>
#include <string>

namespace dse {

// Circuit breaker states.
enum class CircuitState {
    Closed,    // Normal operation, requests allowed
    Open,      // Blocking, requests fail fast
    HalfOpen   // Probing, limited requests allowed
};

// Circuit breaker configuration.
struct CircuitBreakerConfig {
    // Number of consecutive failures to trip the breaker (OPEN).
    std::size_t failure_threshold = 5;

    // How long to stay OPEN before attempting HALF_OPEN (milliseconds).
    std::size_t recovery_timeout_ms = 5000;

    // Number of probe requests allowed in HALF_OPEN state.
    std::size_t half_open_max_probes = 1;
};

// Circuit breaker for protecting against repeated failures.
//
// Usage:
//   CircuitBreaker cb;
//   if (cb.should_allow_request()) {
//       auto result = remote_call();
//       if (result.failed) cb.record_failure();
//       else cb.record_success();
//   } else {
//       // Fail fast — circuit is OPEN
//   }
class CircuitBreaker {
public:
    // Create a circuit breaker with the given configuration.
    explicit CircuitBreaker(CircuitBreakerConfig config = CircuitBreakerConfig{});

    // Check if a request should be allowed.
    // Returns true if the circuit is CLOSED or HALF_OPEN (with available probes).
    // Returns false if the circuit is OPEN.
    bool should_allow_request();

    // Record a successful request.
    // In CLOSED: resets consecutive failure count.
    // In HALF_OPEN: transitions to CLOSED.
    void record_success();

    // Record a failed request.
    // In CLOSED: increments consecutive failure count; trips if threshold exceeded.
    // In HALF_OPEN: transitions to OPEN.
    void record_failure();

    // Get the current state.
    CircuitState state() const;

    // Get the current consecutive failure count (CLOSED state).
    std::size_t consecutive_failures() const;

    // Get configuration.
    const CircuitBreakerConfig& config() const;

    // Reset the circuit breaker to initial state.
    void reset();

private:
    // Check if enough time has elapsed to transition OPEN → HALF_OPEN.
    bool recovery_timeout_elapsed() const;

    // Use unique_ptr to make CircuitBreaker movable (std::mutex is not).
    std::unique_ptr<std::mutex> mutex_;
    CircuitBreakerConfig config_;

    // State
    CircuitState state_ = CircuitState::Closed;
    std::size_t consecutive_failures_ = 0;
    std::size_t half_open_probes_remaining_ = 0;
    std::chrono::steady_clock::time_point opened_at_;
};

} // namespace dse
