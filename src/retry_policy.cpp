// Distributed Search Engine - Retry Policy (Phase 14).
//
// Implementation of the retry policy for bounded retries with
// exponential backoff.

#include "retry_policy.h"

#include <algorithm>
#include <cstddef>
#include <string>

namespace dse {

std::size_t RetryPolicy::delay_for_attempt(std::size_t attempt) const
{
    if (attempt == 0) {
        return 0;  // No delay for the first attempt.
    }

    // Exponential backoff: initial_delay * multiplier^(attempt-1)
    double delay = static_cast<double>(initial_delay_ms);
    for (std::size_t i = 1; i < attempt; ++i) {
        delay *= backoff_multiplier;
    }

    // Cap at max_delay_ms.
    delay = std::min(delay, static_cast<double>(max_delay_ms));

    return static_cast<std::size_t>(delay);
}

bool RetryPolicy::is_retryable(ErrorCategory category) const
{
    switch (category) {
        case ErrorCategory::ConnectionRefused:
        case ErrorCategory::ConnectionReset:
        case ErrorCategory::Timeout:
        case ErrorCategory::TransportError:
            return true;

        case ErrorCategory::InvalidRequest:
        case ErrorCategory::DocumentNotFound:
        case ErrorCategory::DocumentAlreadyExists:
        case ErrorCategory::ServerError:
        case ErrorCategory::Unknown:
            return false;
    }

    return false;
}

ErrorCategory RetryPolicy::classify_error(const std::string& error_message)
{
    // Simple heuristic based on common error patterns.
    // This is intentionally conservative — only classify errors we are
    // confident about.

    if (error_message.find("Connection failed") != std::string::npos ||
        error_message.find("connection refused") != std::string::npos) {
        return ErrorCategory::ConnectionRefused;
    }

    if (error_message.find("Connection reset") != std::string::npos ||
        error_message.find("reset by peer") != std::string::npos) {
        return ErrorCategory::ConnectionReset;
    }

    if (error_message.find("timeout") != std::string::npos ||
        error_message.find("Timeout") != std::string::npos) {
        return ErrorCategory::Timeout;
    }

    if (error_message.find("Request failed") != std::string::npos ||
        error_message.find("Unknown error communicating") != std::string::npos) {
        return ErrorCategory::TransportError;
    }

    if (error_message.find("already exists") != std::string::npos) {
        return ErrorCategory::DocumentAlreadyExists;
    }

    if (error_message.find("not found") != std::string::npos) {
        return ErrorCategory::DocumentNotFound;
    }

    if (error_message.find("Invalid request") != std::string::npos ||
        error_message.find("Missing") != std::string::npos) {
        return ErrorCategory::InvalidRequest;
    }

    if (error_message.find("Server error") != std::string::npos ||
        error_message.find("Internal server error") != std::string::npos) {
        return ErrorCategory::ServerError;
    }

    return ErrorCategory::Unknown;
}

RetryPolicy RetryPolicy::no_retries()
{
    RetryPolicy policy;
    policy.max_attempts = 1;
    return policy;
}

RetryPolicy RetryPolicy::default_policy()
{
    RetryPolicy policy;
    policy.max_attempts = 3;
    policy.initial_delay_ms = 100;
    policy.max_delay_ms = 1000;
    policy.backoff_multiplier = 2.0;
    policy.retry_writes = false;
    return policy;
}

} // namespace dse
