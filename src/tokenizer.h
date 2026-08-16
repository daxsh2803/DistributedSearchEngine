// Distributed Search Engine - Tokenizer (Phase 1B).
//
// Public API only. Design per docs/decisions/ADR-001-tokenizer-design.md:
// a pure, deterministic, ASCII-oriented tokenizer.
//
// The implementation is complete for Phase 1B-2 (see tokenizer.cpp).

#pragma once

#include <string>
#include <string_view>
#include <vector>

namespace dse {

// Converts raw text into normalized tokens.
//
// Contract (ADR-001):
//   - a token is a maximal run of ASCII letters (A-Z, a-z) and ASCII digits
//     (0-9); every other byte is a separator;
//   - ASCII uppercase letters inside a token are lowercased;
//   - tokens are emitted left to right; duplicates are preserved;
//   - empty runs produce no tokens;
//   - deterministic: identical input always yields identical output.
//
// Complexity: O(N) time, O(N) space, single pass over the input.
std::vector<std::string> tokenize(std::string_view input);

} // namespace dse
