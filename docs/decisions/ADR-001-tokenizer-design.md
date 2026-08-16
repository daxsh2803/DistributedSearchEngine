# ADR-001: Tokenizer Design

- **Status:** Accepted
- **Date:** 2026-08-16 (Phase 1A — design; implementation in Phase 1B)
- **Deciders:** DistributedSearchEngine project (per AGENTS.md methodology:
  explain, design, trade-offs, complexity, implement, test, build, report)

## Context

The search engine pipeline begins with converting raw text into indexable
tokens: `Raw Document -> Tokenizer -> Normalized Tokens -> Inverted Index ->
Query Processing -> Ranking -> Results`. The tokenizer is the input gate of
the whole system; every downstream component (index, query processing,
BM25-style ranking) consumes its output, so its decisions define the engine's
vocabulary and directly affect term frequencies, matching, and ranking.

Phase 1 introduces this first real component. The constraints:

- C++20, no third-party text-processing libraries;
- the core engine must be implemented by us (no Lucene/Solr/Elasticsearch);
- keep the component simple, deterministic, and thoroughly testable;
- keep the project buildable and testable after every phase;
- future phases will add an inverted index and BM25 ranking, which require
  term-frequency information;
- ranking must remain free to evolve (stop words, stemming, etc. are not
  decisions this phase should lock in).

## Decision

Adopt a **pure, ASCII-oriented tokenizer** with a single, deterministic rule:

> A token is a maximal run of ASCII letters (`A`-`Z`, `a`-`z`) and ASCII
> digits (`0`-`9`). Every other byte is a separator. ASCII uppercase letters
> inside a token are lowercased. Tokens are emitted left to right; duplicates
> are preserved; empty runs produce no tokens.

**API (proposal):** `std::vector<std::string> dse::tokenize(std::string_view input);`

- Input `std::string_view` (non-owning, zero-copy; safe because the function
  never stores it).
- Output `std::vector<std::string>` (owning, ordered, duplicate-preserving,
  mutable, easy to test).
- Free function in namespace `dse`; no class, no state, no options in Phase 1.
- Implementation: single linear scan; O(N) time, O(N) space.
- Classification is explicit (ASCII range checks), not locale-dependent
  (`std::isspace`/`std::ispunct`), so behavior is identical on every
  platform and locale. Lowercasing uses `std::tolower` with the mandatory
  `unsigned char` cast.

## Tokenization rules (Phase 1, canonical)

| Input | Tokens |
|---|---|
| `hello world` | `hello`, `world` |
| `Search SEARCH search` | `search`, `search`, `search` |
| `hello     world` | `hello`, `world` |
| `   hello world   ` | `hello`, `world` |
| `hello, world!` | `hello`, `world` |
| `search-engine` | `search`, `engine` |
| `""` / whitespace only | (empty) |
| `cat cat dog` | `cat`, `cat`, `dog` |
| `42` / `007` | `42` / `007` |
| `3.14` | `3`, `14` (decimal splits) |
| `$9.99` | `9`, `99` |
| `abc123` | `abc123` (digits stay inside tokens) |
| `a!!!b` / `...` | `a`, `b` / (empty) |
| `don't` | `don`, `t` |
| `under_score` | `under`, `score` |
| `and/or` | `and`, `or` |
| `C++` | `c` |
| `rock&roll` | `rock`, `roll` |
| `user@example.com` | `user`, `example`, `com` |
| `https://example.com/path?q=1` | `https`, `example`, `com`, `path`, `q`, `1` |
| `café` | `caf` (non-ASCII bytes are separators; documented limitation) |

Full edge-case table: `docs/learning/phase-1-tokenizer.md`, Section 13.

## Alternatives considered

1. **Output as `std::vector<std::string_view>`** — avoids per-token
   allocations, but views would borrow the caller's buffer (dangling risk
   with temporaries) and cannot hold lowercased text without new storage.
   Rejected: owning strings are the honest representation.
2. **Input as `std::string`** — forces an unnecessary copy of the whole
   input; buys nothing because the view is never stored. Rejected.
3. **Tokenizer class with state/options** — premature: Phase 1 has no
   configuration to carry (no stop words, no languages, no stemming).
   Revisited when options exist; kept additive.
4. **Digits as separators** — would drop `42` and `iphone 15`-style terms
   entirely. Rejected: digits stay part of tokens (a query for `top 10`
   must match `top 10`).
5. **Hyphen joins the parts** (`search-engine` -> `searchengine`) — more
   sophisticated for some compounds, but a second, arbitrary rule; splitting
   is the simpler deterministic default. Revisited if compound handling is
   needed.
6. **Unicode-aware tokenization now** — requires UTF-8 decoding, code-point
   categories, normalization, and per-script rules; a large scope increase
   for the first component. Rejected for Phase 1; documented as a known
   limitation and a future replacement policy.
7. **Locale-based classification (`std::isspace`/`std::ispunct`)** — results
   depend on the C locale; breaks determinism across environments. Rejected
   in favor of explicit ASCII checks.
8. **Stop-word removal / stemming / lemmatization inside the tokenizer** —
   these are retrieval and language *policies* that should be separate,
   configurable pipeline stages (see Consequences). Rejected.

## Trade-offs

Accepted costs of the chosen design:

- **ASCII only** — non-ASCII text (e.g., `café`, `你好`) loses characters or
  yields nothing; no accents, no multilingual support yet.
- **Decimal numbers split** (`3.14` -> `3`, `14`) — numeric queries lose
  structure; a future numeric-handling stage can repair this additively.
- **Contractions and compounds split** (`don't` -> `don`, `t`; `C++` -> `c`;
  email/URLs fragment) — predictable but linguistically crude.
- **Per-token allocation** in the output — acceptable at Phase 1 scale; a
  streaming/arena design is a possible future optimization.

Benefits:

- one deterministic, locale-independent rule — trivially specified, tested,
  and ported;
- pure function — no state, no I/O: trivially testable, reproducible,
  parallelizable;
- O(N) single-pass — scales to large documents and enables streaming later;
- duplicates preserved — term frequencies survive for TF-IDF/BM25;
- the API is minimal and honest about ownership — future features remain
  additive.

## Consequences

Positive:

- The tokenizer becomes a leaf component: it depends only on the standard
  library; future components depend on it (dependency direction stays
  clean).
- Documents and queries are tokenized by the *same* rule, so matching is
  consistent by construction.
- The exact behavioral contract lives in one place (the rule + the
  Section 13 table), ready to become executable tests in Phase 1B.
- Later stages (stop-word filter, stemming, lemmatization, synonym
  expansion) slot in *after* the tokenizer without changing its contract,
  and can be enabled/disabled independently for experimentation.

Negative (accepted):

- The engine is not multilingual and loses some text fidelity (accents,
  decimals, contractions, URLs). This is a documented Phase 1 limitation,
  not a hidden defect.

## Future evolution

All extensions are additive and do not break the Phase 1 contract:

- Unicode tokenization as a replacement policy inside the same stage
  (contract unchanged);
- configurable stop-word filtering, stemming, lemmatization as separate
  downstream stages;
- token metadata (positions/offsets) via a new function
  (`tokenize_with_positions`) rather than changing `tokenize`;
- streaming tokenization, enabled by the single-pass design;
- options/class form only when real configuration exists.

Reference: full learning material in
`docs/learning/phase-1-tokenizer.md`.
