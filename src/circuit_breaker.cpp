// Distributed Search Engine - Circuit Breaker (Phase 15).
//
// Implements the state machine for circuit breaker pattern.
// All state transitions are protected by a mutex for thread safety.

#include "circuit_breaker.h"

#include <algorithm>
#include <chrono>

namespace dse {

CircuitBreaker::CircuitBreaker(CircuitBreakerConfig config)
    : mutex_(std::make_unique<std::mutex>())
    , config_(config)
{
}

bool CircuitBreaker::should_allow_request()
{
    std::lock_guard<std::mutex> lock(*mutex_);

    switch (state_) {
        case CircuitState::Closed:
            return true;

        case CircuitState::Open:
            // Check if recovery timeout has elapsed.
            if (recovery_timeout_elapsed()) {
                // Transition to HALF_OPEN.
                state_ = CircuitState::HalfOpen;
                half_open_probes_remaining_ = config_.half_open_max_probes;
                // This first probe counts as one of the allowed probes.
                if (half_open_probes_remaining_ > 0) {
                    --half_open_probes_remaining_;
                    return true;  // Allow the probe request.
                }
                return false;  // No probes configured.
            }
            return false;  // Still OPEN, fail fast.

        case CircuitState::HalfOpen:
            // Allow limited probe requests.
            if (half_open_probes_remaining_ > 0) {
                --half_open_probes_remaining_;
                return true;
            }
            return false;  // No probes remaining, fail fast.
    }

    return false;  // Should not reach here.
}

void CircuitBreaker::record_success()
{
    std::lock_guard<std::mutex> lock(*mutex_);

    switch (state_) {
        case CircuitState::Closed:
            // Reset consecutive failure count.
            consecutive_failures_ = 0;
            break;

        case CircuitState::Open:
            // Should not happen (requests are blocked), but handle gracefully.
            break;

        case CircuitState::HalfOpen:
            // Probe succeeded — transition to CLOSED.
            state_ = CircuitState::Closed;
            consecutive_failures_ = 0;
            break;
    }
}

void CircuitBreaker::record_failure()
{
    std::lock_guard<std::mutex> lock(*mutex_);

    switch (state_) {
        case CircuitState::Closed:
            // Increment consecutive failure count.
            ++consecutive_failures_;
            if (consecutive_failures_ >= config_.failure_threshold) {
                // Threshold exceeded — transition to OPEN.
                state_ = CircuitState::Open;
                opened_at_ = std::chrono::steady_clock::now();
            }
            break;

        case CircuitState::Open:
            // Already OPEN, no state change.
            break;

        case CircuitState::HalfOpen:
            // Probe failed — transition back to OPEN.
            state_ = CircuitState::Open;
            opened_at_ = std::chrono::steady_clock::now();
            break;
    }
}

CircuitState CircuitBreaker::state() const
{
    std::lock_guard<std::mutex> lock(*mutex_);
    return state_;
}

std::size_t CircuitBreaker::consecutive_failures() const
{
    std::lock_guard<std::mutex> lock(*mutex_);
    return consecutive_failures_;
}

const CircuitBreakerConfig& CircuitBreaker::config() const
{
    return config_;
}

void CircuitBreaker::reset()
{
    std::lock_guard<std::mutex> lock(*mutex_);
    state_ = CircuitState::Closed;
    consecutive_failures_ = 0;
    half_open_probes_remaining_ = 0;
}

bool CircuitBreaker::recovery_timeout_elapsed() const
{
    auto now = std::chrono::steady_clock::now();
    auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
        now - opened_at_).count();
    return static_cast<std::size_t>(elapsed) >= config_.recovery_timeout_ms;
}

} // namespace dse
