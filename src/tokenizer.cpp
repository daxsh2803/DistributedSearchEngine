// Distributed Search Engine - Tokenizer (Phase 1B-2).
//
// Implementation of the contract in docs/decisions/ADR-001-tokenizer-design.md:
//
//   A token is a maximal run of ASCII letters (A-Z, a-z) and ASCII digits
//   (0-9). Every other byte is a separator. ASCII uppercase letters inside
//   a token are lowercased. Tokens are emitted left to right; duplicates
//   are preserved; empty runs produce no tokens.
//
// The function is pure and deterministic: no state, no I/O, no locale
// dependence. It performs exactly one linear pass over the input: O(N) time,
// O(N) output space. The input view is read but never stored.

#include "tokenizer.h"

namespace dse {

namespace {

// ASCII-only token-character classification. Deliberately explicit range
// checks instead of locale-dependent <cctype> functions (std::isalnum etc.),
// so behavior is identical on every platform and locale.
// The parameter is unsigned char so values >= 0x80 never become negative.
bool is_token_char(unsigned char c)
{
    return (c >= 'A' && c <= 'Z') ||
           (c >= 'a' && c <= 'z') ||
           (c >= '0' && c <= '9');
}

// ASCII-only uppercase normalization: 'A'-'Z' -> 'a'-'z'; everything else
// (lowercase letters, digits, non-token bytes) is returned unchanged.
// Explicit range check - no locale-dependent character conversion.
unsigned char to_ascii_lower(unsigned char c)
{
    if (c >= 'A' && c <= 'Z') {
        return static_cast<unsigned char>(c + ('a' - 'A'));
    }
    return c;
}

} // namespace

std::vector<std::string> tokenize(std::string_view input)
{
    std::vector<std::string> tokens;
    std::string current;

    for (const char raw : input) {
        const unsigned char c = static_cast<unsigned char>(raw);

        if (is_token_char(c)) {
            current.push_back(static_cast<char>(to_ascii_lower(c)));
        } else {
            // Separator: flush the token accumulated so far, if any.
            if (!current.empty()) {
                tokens.push_back(std::move(current));
                current.clear();
            }
        }
    }

    // Flush the trailing token, if any.
    if (!current.empty()) {
        tokens.push_back(std::move(current));
    }

    return tokens;
}

} // namespace dse
