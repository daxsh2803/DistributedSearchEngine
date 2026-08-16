# Project Status

## Current Phase

Phase 2 — Inverted Index

- Phase 2A — Design and Learning: **COMPLETE**
- Phase 2B — Implementation: **PENDING**

## Completed: Phase 1

Phase 1 — Text Processing and Tokenization: **COMPLETE**

- Phase 1A — Design and Learning: **COMPLETE**
- Phase 1B-1 — Core Library Structure: **COMPLETE**
- Phase 1B-2 — Tokenizer Implementation and Tests: **COMPLETE**

## Completed

- [x] Development environment configured
- [x] GCC 16.2 configured (MSYS2 UCRT64, `C:\msys64\ucrt64\bin\g++.exe`)
- [x] C++20 support verified
- [x] CMake 4.4.2 installed (MSYS2 UCRT64)
- [x] Ninja 1.13.2 installed (MSYS2 UCRT64)
- [x] Git installed
- [x] GitHub repository created
- [x] Local Git repository initialized
- [x] GitHub remote configured
- [x] Project directory structure created
- [x] CMake project created
- [x] GoogleTest configured
- [x] Initial test passing
- [x] Initial commit created (commit `ea895c4`, verified locally)
- [x] Initial push completed (per user confirmation)
- [x] Phase 1A — tokenizer design specification and learning documentation
      (`docs/learning/phase-1-tokenizer.md`, `docs/decisions/ADR-001-tokenizer-design.md`)
- [x] Phase 1B-1 — core library structure (`dse_core` static library, tokenizer
      header/source, `tokenizer_test` target, CMake wiring)
- [x] Phase 1B-2 — tokenizer implementation and tests (`dse::tokenize` per
      ADR-001; 45 tests across the eight Phase 1A test groups)
- [x] Phase 2A — inverted index design specification and learning
      documentation (`docs/learning/phase-2-inverted-index.md`,
      `docs/decisions/ADR-002-inverted-index-design.md`)

## Verification Results (Phase 0)

All results below were actually obtained on this machine on 2026-08-16 using
this project's verified toolchain: MSYS2 UCRT64 with GCC 16.2.0, CMake 4.4.2,
Ninja 1.13.2, and C++20. The CodeBlocks MinGW GCC 8.1 installation that sits
on the default PATH must **not** be used — it cannot compile C++20. Always run
CMake with the UCRT64 `bin` directory first on PATH:

```bash
export PATH="/c/msys64/ucrt64/bin:$PATH"
```

### Build

```
cmake -S . -B build -G Ninja \
  -DCMAKE_CXX_COMPILER=/c/msys64/ucrt64/bin/g++.exe \
  -DCMAKE_MAKE_PROGRAM=/c/msys64/ucrt64/bin/ninja.exe
cmake --build build
```

Result: success (8/8 Ninja steps). Built `DistributedSearchEngine.exe`,
`smoke_test.exe`, and the GoogleTest static libraries (`libgtest.a`,
`libgtest_main.a`). GoogleTest `v1.18.0` was fetched at configure time via
FetchContent (not vendored into the repository).

### Tests

```
ctest --test-dir build --output-on-failure
```

Result: **2/2 tests passed (100%)** — `SmokeTest.AddOneIncrementsValue` and
`SmokeTest.Cpp20FeatureAvailable`; total test time ~0.2 s. GoogleTest is
integrated and CTest discovers and executes the tests.

### Application

```
./build/DistributedSearchEngine.exe
```

Result: prints `DistributedSearchEngine | Phase 0 - Project Foundation` and
`Built with C++ standard: 202002`; exit code 0.

## Phase 1A (Design & Learning) — Completed

- [x] Search-engine context and tokenizer responsibility documented
- [x] API contract proposed (`std::string_view` input, owning `std::vector<std::string>` output)
- [x] Deterministic ASCII tokenization rules defined (maximal runs of `[A-Za-z0-9]`,
      lowercased; everything else a separator)
- [x] Unicode scope documented as a known limitation (ASCII-only for Phase 1)
- [x] Testing strategy designed (groups defined; no test files created yet)
- [x] Learning material: `docs/learning/phase-1-tokenizer.md`
- [x] Architecture decision record: `docs/decisions/ADR-001-tokenizer-design.md`

## Phase 1B (Implementation) — Completed

- [x] Phase 1B-1 — `dse_core` static library created; `DistributedSearchEngine`
      and `tokenizer_test` link against it; include directories configured
      (PUBLIC) so `#include "tokenizer.h"` works for all consumers
- [x] Phase 1B-2 — `dse::tokenize` implemented exactly per ADR-001: single-pass
      O(N), explicit ASCII range checks (`is_token_char`, `to_ascii_lower`),
      no third-party code
- [x] `tokenizer_test.cpp` covers the eight Phase 1A test groups
      (basic, normalization, punctuation, whitespace, hyphen, duplicates,
      numbers/symbols, edge cases)

### Phase 1B Verification Results (actual, 2026-08-16, UCRT64 toolchain)

- Build: **successful, zero warnings** (`-Wall -Wextra -Wpedantic`)
- Tests: **47/47 passed (100%)** — 45 tokenizer tests + 2 Phase 0 smoke tests
- Total test time: ~1.25 s (functional check; **no performance benchmark was
  performed**)
- Application: `DistributedSearchEngine.exe` ran with exit code 0,
  `__cplusplus == 202002` (C++20)

Full details: `docs/learning/phase-1-tokenizer.md`, section
"Implementation Results — Phase 1B".

## Phase 2A (Design & Learning) — Completed

- [x] Inverted-index fundamentals and search-engine pipeline context documented
- [x] Data model designed (`doc_id`, `Posting {document_id, term_frequency}`,
      postings lists always sorted by document ID)
- [x] API proposed (`InvertedIndex` class: `add_document`, `postings`,
      `document_count`, `term_count`, `contains`)
- [x] Lookup semantics defined (`std::span<const Posting>`, empty for missing
      terms, documented lifetime contract)
- [x] Insertion semantics defined (unique docID precondition; duplicates
      aggregated into term frequency)
- [x] `unordered_map` vs `map` trade-offs analyzed; `unordered_map` chosen
- [x] Complexity analyzed (O(1) average lookup; O(T) amortized per document;
      O(distinct term-doc pairs) space)
- [x] Edge-case table and testing strategy designed (12 test groups planned)
- [x] Phase 1 → Phase 2 integration documented (index depends on the
      tokenizer; both live in `dse_core`)
- [x] Learning material: `docs/learning/phase-2-inverted-index.md`
- [x] Architecture decision record: `docs/decisions/ADR-002-inverted-index-design.md`

## Next Phase

Phase 3 — Query Processing (per the pipeline documented in the phase
learning material: Tokenizer → Inverted Index → Query Processing → Ranking
→ Results; Phase 3 details to be designed in Phase 3A)