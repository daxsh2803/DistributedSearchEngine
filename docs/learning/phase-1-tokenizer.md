# Phase 1 - Tokenizer: Design and Learning Notes (Phase 1A)

Phase 0 gave us a buildable, testable C++20 project with GoogleTest wired in.
Phase 1 introduces the first real search-engine component: the **tokenizer**.
This document is the Phase 1A design specification and learning material. It
is written for a C++ student learning system design, so the ideas are
*explained* rather than just listed. The actual code comes in Phase 1B; this
document deliberately contains **no implementation**.

The companion decision record is
`docs/decisions/ADR-001-tokenizer-design.md`, which captures *why* these
choices were made.

---

## 1. Where the tokenizer fits in the search engine

Every search engine, no matter how sophisticated, starts the same way: it
must turn free text into something it can index. The conceptual pipeline:

```
Raw Document
    |
    v
Tokenizer
    |
    v
Normalized Tokens
    |
    v
Inverted Index
    |
    v
Query Processing
    |
    v
Ranking
    |
    v
Results
```

- **Raw Document** - the original text (a file, a web page, a user query).
  It contains everything: case, punctuation, whitespace, symbols, noise.
- **Tokenizer** - converts that raw text into a list of *normalized tokens*
  (roughly, "words"). This is our component.
- **Normalized Tokens** - a clean, predictable list of terms such as
  `["search", "engine"]`.
- **Inverted Index** - the data structure that maps each term to the
  documents (and positions) where it appears. This is built *from* the
  tokens.
- **Query Processing** - turns a user's query into tokens and looks up the
  matching terms in the index.
- **Ranking** - orders matching documents by relevance (later: BM25).
- **Results** - the final ordered list returned to the user.

### Why the tokenizer is foundational

The tokenizer is the **input gate**: every downstream component consumes its
output. Two consequences follow:

1. **Its decisions shape the vocabulary.** What counts as a "word" in this
   engine is *defined* by the tokenizer. If `Search` and `search` were
   different tokens, a query for `search` would miss documents containing
   `Search`. If the tokenizer does not lower-case, every capitalization
   variant becomes a separate index term and the index bloats while recall
   collapses.
2. **Errors here cannot be fixed later.** If the tokenizer drops or mangles
   words, the index never contains them, so no amount of clever ranking can
   ever retrieve them. This is the "garbage in, garbage out" principle: the
   tokenizer is the cheapest place to get things right, and the most
   expensive place to get them wrong.

Tokenization decisions also directly affect **ranking**: whether
`search-engine` and `search engine` are the same term, whether duplicates are
preserved (term frequencies!), whether stop words exist in the vocabulary —
all of these change what the index and the ranker see. So although the
tokenizer is a small component, it is one of the most consequential ones in
the whole system.

---

## 2. The tokenizer's responsibility

A good component has a **single, clearly stated job**. Our job:

> **Convert a raw text string into a list of normalized token strings.**

### The tokenizer SHOULD

- split text into tokens;
- normalize ASCII alphabetic characters to lowercase;
- handle whitespace (spaces, tabs, newlines) as token separators;
- remove punctuation according to clearly defined rules;
- split hyphenated words into separate tokens;
- preserve token order;
- preserve duplicate tokens;
- ignore empty tokens (they simply do not exist).

### The tokenizer should NOT

- build an inverted index;
- rank documents;
- perform query processing;
- remove stop words;
- perform stemming;
- perform lemmatization;
- perform synonym expansion;
- perform spell correction;
- perform distributed processing;
- perform persistence;
- perform networking.

### Why these responsibilities stay outside

Each of those excluded jobs is a **separate transformation with its own
correctness criteria and its own future**:

- *Indexing, ranking, query processing* are later pipeline stages that
  consume tokens. Putting them in the tokenizer would entangle "what is a
  word" with "how we search", making both impossible to test or change
  independently.
- *Stop-word removal* is a retrieval *policy* decision (see Section 7). It is
  not a property of words; it is a property of a corpus and a language.
- *Stemming / lemmatization / synonyms / spelling* are language-processing
  stages that change token *identity*. Each is complex, language-specific,
  and best developed and measured on its own.
- *Distribution, persistence, networking* are infrastructure concerns
  unrelated to turning text into tokens.

The core architectural idea is **separation of concerns**: the tokenizer
produces a clean, deterministic list of tokens, and *anything* that wants to
change that list (filter, stem, expand) does so in a later, separate stage.
This keeps each piece simple, testable, and replaceable.

---

## 3. Input / output contract

### Input: `std::string` or `std::string_view`?

Proposed: `std::string_view`.

- `std::string_view` is a **non-owning view** (a pointer + a length). Passing
  it does not copy the text. Callers can hand us a `std::string`, a `const
  char*`, a string literal, or even a substring of a larger buffer — all
  without any allocation.
- The only cost is a **lifetime obligation**: the buffer the view points to
  must stay alive *for the duration of the call*. That is trivially satisfied
  here because the tokenizer only reads the input during the call and never
  stores it. We never retain the view, so no dangling risk escapes the
  function.
- `std::string` (owning) would force every caller to materialize a full copy
  of the text just to call us — a pure waste, since we only read it. The one
  thing an owning parameter "protects" against is passing a temporary whose
  storage dies after the call — but since we do not store the view, that
  protection buys us nothing.

Trade-off summary: `string_view` = zero-copy input, fast, flexible; the
lifetime rule is easy to honor because we never store it.

### Output: what type, and why

Proposed: `std::vector<std::string>`.

- **Ownership**: the vector owns its buffer and each `std::string` owns its
  characters. The caller receives fully independent, valid data. Nothing
  references the caller's input afterward.
- **Lifetime**: controlled entirely by the caller; the result lives as long
  as the caller keeps the vector.
- **Mutability**: fully mutable; the caller may reorder, filter, or inspect
  tokens freely.
- **Performance**: tokens are stored contiguously; iterating and indexing are
  cache-friendly. Each token string is a small heap allocation (or uses the
  compiler's small-string optimization for short words) — an acceptable cost
  at this scale, and one we can revisit later with a streaming or arena
  design (see Section 16).
- **Ease of testing**: comparing a whole `vector<string>` against an expected
  vector is a single assertion.

The natural alternative, `std::vector<std::string_view>`, is *tempting* (no
per-token allocations) but wrong for us: the views would point into the
caller's buffer (dangerous if the caller passed a temporary), and — the
decisive point — we must **lowercase** tokens, which requires producing *new*
characters anyway. An owning output is the honest representation of what the
function does.

### Proposed signature (proposal only, not implemented in Phase 1A)

```cpp
namespace dse {

// Converts raw text into normalized tokens.
// Deterministic: identical input always yields identical output.
std::vector<std::string> tokenize(std::string_view input);

} // namespace dse
```

This is a **pure function**: no state, no I/O, no randomness, no failure
modes beyond memory allocation. That single fact makes it trivial to test and
reason about.

---

## 4. Tokenization rules

The whole behavior reduces to one canonical rule:

> **A token is a maximal run of ASCII letters (`A`-`Z`, `a`-`z`) and ASCII
> digits (`0`-`9`). Every other byte is a separator. ASCII uppercase letters
> inside a token are lowercased. Tokens are emitted left to right, and
> duplicates are preserved.**

That rule, and nothing else, produces every case below.

### A. Normal words

```
"hello world"  ->  ["hello", "world"]
```

### B. Case normalization

```
"Search SEARCH search"  ->  ["search", "search", "search"]
```

All three tokens lower-case to the same term. This is what makes queries
case-insensitive at index time.

### C. Multiple whitespace

```
"hello     world"  ->  ["hello", "world"]
```

Any run of separators — no matter how long — produces exactly one break.

### D. Leading/trailing whitespace

```
"   hello world   "  ->  ["hello", "world"]
```

Separators at the edges produce no tokens.

### E. Punctuation

```
"hello, world!"  ->  ["hello", "world"]
```

`,` and `!` are separators; the words survive.

### F. Hyphenated words

```
"search-engine"  ->  ["search", "engine"]
```

The hyphen is a separator, so it *splits* the word into two tokens. (This is
a deliberate Phase 1 choice — see Section 5.)

### G. Empty input

```
""  ->  []
```

### H. Whitespace-only input

```
"   \t\n   "  ->  []
```

### I. Duplicate words

```
"cat cat dog"  ->  ["cat", "cat", "dog"]
```

### Why duplicates MUST be preserved

Ranking algorithms such as **TF-IDF** and **BM25** are built on **term
frequency**: how many times a term appears in a document. If "cat" appears
twice, the index must know the count is 2, not 1. If the tokenizer silently
deduplicated, every document would look as if each word appeared once, and
the ranker would be blind to emphasis, repetition, and topic density. In
short: **duplicates are information**, and discarding them would corrupt the
very signal ranking is based on. The index (not the tokenizer) is where term
frequencies get counted — but it can only count what the tokenizer passes
through.

---

## 5. Numbers and special characters

Phase 1 policy, from the canonical rule:

- **Integers** are kept as tokens: `"42" -> ["42"]`, `"007" -> ["007"]`
  (leading zeros preserved).
- **Decimal numbers** are *split* at the decimal point:
  `"3.14" -> ["3", "14"]`. The decimal point is a separator. This is a known
  simplification, not a bug.
- **Punctuation around numbers** acts as separators:
  `"$9.99" -> ["9", "99"]`, `"(2024)" -> ["2024"]`, `"1,000" -> ["1", "000"]`.
- **Signs** are separators: `"+5" -> ["5"]`, `"-5" -> ["5"]`.
- **Symbols** (`@ # $ % ^ & * = + < > ~ \` | ...) are all separators.
- **Repeated punctuation** collapses into a single separator run:
  `"a!!!b" -> ["a", "b"]`, `"..." -> []`.
- **Underscores** are separators: `"under_score" -> ["under", "score"]`.
- **Apostrophes** are separators: `"don't" -> ["don", "t"]`,
  `"rock'n'roll" -> ["rock", "n", "roll"]`, `"'tis" -> ["tis"]`.
- **Slash** is a separator: `"and/or" -> ["and", "or"]`, `"1/2" -> ["1", "2"]`.
- **Plus sign** is a separator: `"C++" -> ["c"]`, `"a+b" -> ["a", "b"]`.
- **Ampersand** is a separator: `"rock&roll" -> ["rock", "roll"]`.
- **Email-like text** fragments: `"user@example.com" -> ["user", "example", "com"]`.
- **URLs** fragment: `"https://example.com/path?q=1" -> ["https", "example", "com", "path", "q", "1"]`.

The trade-offs of this simple ASCII-oriented policy:

| We gain | We give up |
|---|---|
| One deterministic, locale-independent rule | Decimal numbers lose their point (`3.14` → `3`, `14`) |
| No parsing "smartness" that can silently disagree | Contractions split (`don't` → `don`, `t`) |
| Trivial to specify, test, and port | Email addresses and URLs fragment into pieces |
| Same rule for documents and queries | `C++` becomes the token `c` |

All of these are *acceptable* for Phase 1: they are predictable, documented,
and the pipeline still functions. A later phase can add smarter handling
(e.g., keeping decimals whole) without changing the tokenizer's contract — it
would be a separate, opt-in stage.

---

## 6. Unicode scope

### Why Unicode tokenization is hard

- **UTF-8 is variable-width.** A code point ("character") occupies 1–4
  bytes. Slicing "characters" requires decoding byte sequences; you cannot
  just walk bytes. `é` is two bytes, `😀` is four.
- **Bytes ≠ code points ≠ graphemes.** What a user perceives as one
  "character" (e.g. `é` written as `e` + combining accent) can be multiple
  code points. Token boundaries become ambiguous.
- **Unicode categories.** "Is this a letter?" spans over 150,000 code points
  across dozens of scripts, and the answer is not "A-Z". There is no simple
  ASCII-style range check.
- **Case folding is script- and language-dependent.** German `ß` folds to
  `ss`; Turkish has a dotted capital `İ` that lowercases to `i̇` (with a
  combining dot); Greek `Σ` lowercases to `σ` mid-word but `ς` at word end.
  "Just lowercase it" is not well-defined.
- **Normalization.** `é` can be encoded as one code point (NFC) or as `e` +
  combining mark (NFD). Two texts that look identical can tokenize
  differently unless normalized first.
- **No spaces in some languages.** Chinese, Japanese, and Thai do not use
  spaces between words; "tokenization" there is a segmentation problem that
  needs dictionaries or statistical models — a completely different beast.

### Phase 1 scope (explicit)

**Phase 1 tokenizes ASCII only.** By the canonical rule, any byte ≥ `0x80`
(which includes *every* byte of a UTF-8 multi-byte sequence) is treated as a
separator. Consequences:

- ASCII text behaves exactly as specified in Sections 4–5.
- Non-ASCII text is **not garbled** (we never split inside a multi-byte
  sequence — we treat the whole sequence as a separator), but it is
  **discarded**: `"café" -> ["caf"]`, `"naïve" -> ["na", "ve"]`, and
  `"你好" -> []`.

This is a **documented known limitation**, not an attempt at production-grade
multilingual tokenization. We deliberately choose correctness-by-construction
for ASCII over partial, inconsistent support for everything else. Unicode
tokenization is a future extension (Section 16), designed as a *replacement
policy inside the same stage* rather than bolted onto Phase 1.

---

## 7. Stop words

**Stop words** are common words — `the`, `is`, `a`, `an`, `of`, `on` — that
carry little meaning on their own and appear in almost every document. A
common (and often misguided) optimization is to delete them at index time so
the index stays small and "noise" terms don't pollute ranking.

### Why Phase 1 does NOT remove stop words

1. **It is a policy, not a fact.** Whether `the` should be indexed depends on
   the corpus, the language, and the query. There is no objective "stop word"
   — only decisions.
2. **It can hurt retrieval.** Phrase and exact-match queries such as
   `"to be or not to be"` or `"the who"` *need* their stop words. Once
   removed at index time, they can never be searched again.
3. **It changes ranking inputs.** BM25-style ranking treats rare terms as
   more informative. Removing `the` globally is usually harmless; removing
   terms by a fixed list can distort scores for documents that genuinely use
   those words.
4. **It belongs to a separate stage.** Filtering is a transformation applied
   *after* tokenization, and it should be configurable (per language, per
   use-case). The tokenizer should not hard-code it.

### The architectural advantage of separation

Keeping **tokenization** and **stop-word filtering** as separate stages means:

- the tokenizer's contract stays pure: "everything becomes tokens";
- later, we can add a filter that runs *only at index time*, *only at query
  time*, or *differently in each*, without touching a single line of
  tokenizer code;
- we can experiment with "do stop words help or hurt?" on real data simply
  by enabling/disabling the filter — no re-tokenization, no contract change.

The Phase 1 pipeline deliberately ends at the tokenizer. A stop-word filter
is a Phase-1-or-later *downstream* stage, not part of this component.

---

## 8. Stemming and lemmatization

- **Stemming** is a crude, rule-based process that chops word endings to a
  common "stem": `running -> run`, `runs -> run`, `ran -> run` (via rules
  like "remove -ing, -s"). It is fast, language-specific, and often produces
  strings that are not real words (Porter stemmer: `relational -> relat`).
- **Lemmatization** is a dictionary/grammar-based process that reduces a word
  to its dictionary form (the *lemma*), considering part of speech:
  `running (verb) -> run`, `better (adjective) -> good`, `mice -> mouse`.
  It is slower and needs linguistic resources.

| | Stemming | Lemmatization |
|---|---|---|
| Method | heuristic rules | dictionary + grammar |
| Output | may not be a real word | always a real word |
| Needs part of speech? | no | usually yes |
| Speed | fast | slower |
| Example | `cats -> cat`, `running -> run` | `mice -> mouse`, `better -> good` |

### Why neither belongs in the Phase 1 tokenizer

- They **change token identity** in language-specific, lossy, and
  occasionally wrong ways. That is a *semantic* transformation, not a
  *lexical* one — a different concern from splitting and lowercasing.
- They are **language-dependent**; the tokenizer currently knows nothing
  about language, and hard-coding English stemming into it would make it
  impossible to reuse for another language.
- Phase 1 has **no index yet** — there is nothing to measure the benefit
  against. Introducing lossy transformations before the index exists would
  optimize blindly.
- They belong to a **later pipeline stage** (post-tokenization, likely
  post-stop-word-filtering), where their effects can be measured and
  toggled independently.

---

## 9. Data structures

### Why `std::vector<std::string>` fits the output

- **Ordered output.** A vector is a sequence; element *i* is the *i*-th token
  in document order. Order matters for phrase queries and positions later.
- **Duplicate preservation.** A vector keeps every element; it never dedupe
  or sorts. Exactly what Section 4.I requires.
- **Contiguous storage.** Tokens are laid out one after another; iterating,
  indexing (`tokens[7]`), and passing to algorithms are cache-friendly and
  fast.
- **Iteration.** Range-based `for (const auto& t : tokens)` and the whole
  STL algorithm family (`std::find`, `std::transform`, ...) work directly.
- **Memory usage.** The vector holds `#tokens` `std::string`s. Each string
  either uses the small-string optimization (short words — no heap
  allocation at all) or a small heap buffer sized to the word. Total token
  characters can never exceed the input length (we only ever *remove*
  bytes), so memory is bounded by `O(N)` plus vector capacity overhead.

### Alternatives considered, and why they lose

- `std::vector<std::string_view>` — non-owning; would dangle with temporary
  inputs and cannot hold lowercased text without a place to put it
  (Section 3). Rejected.
- `std::list` / `std::deque` — no contiguity, no benefit here. Rejected.
- `std::set` / `std::unordered_set` — destroy order and duplicates; exactly
  wrong for our contract. Rejected.
- Custom token arena / intrusive storage — would save allocations but adds
  complexity and coupling; a premature optimization at this phase. Rejected
  (see Section 16 for when it could return).

---

## 10. C++ concepts you need for the implementation

These are the C++20 building blocks the tokenizer (Phase 1B) will use.

### `std::string`

An **owning** character container. It allocates and frees its own buffer and
copies on assignment. Short strings use the *small-string optimization*
(SSO): no heap allocation at all. Value semantics: `auto b = a;` copies.

```cpp
std::string s = "hello";
s.push_back('!');      // s == "hello!"
std::string copy = s;  // independent copy
```

### `std::string_view`

A **non-owning** view: just a pointer and a length. Cheap to pass and copy
(no allocation). It *borrows* characters that someone else owns — that owner
must outlive the view. Converting between the two is free:

```cpp
std::string s = "abc";
std::string_view v = s;          // view borrows s's buffer
std::string back = std::string(v); // copy out of the view
```

The rule that keeps `string_view` safe in our API: **a view must not outlive
the buffer it points to**. Our `tokenize` reads the view and returns owning
strings, so the rule is satisfied automatically.

### `std::vector`

A **contiguous dynamic array**. `push_back` appends in amortized `O(1)`
(growing by reallocation when needed). Elements are copied/moved in; the
vector owns them and destroys them when it dies (RAII).

```cpp
std::vector<std::string> tokens;
tokens.push_back("search");   // copies "search" into the vector
tokens.emplace_back("engine"); // constructs in place
```

### `const`

Declares something that cannot be modified, and *documents intent*. In the
implementation you will see it everywhere:

```cpp
const std::string_view input;          // tokenize never modifies its input
for (const char c : input) { ... }     // reading, not writing
```

`const` correctness (adding `const` wherever a value is only read) catches
accidental mutation at compile time and makes the "we never modify the
input" promise enforceable.

### References

An alias for an existing object — no copy, no pointer syntax. Use
`const T&` for "read-only, no copy" parameters; the tokenizer's function
takes `const std::string_view&`-style semantics (views are cheap enough that
passing by value is idiomatic, but the *const* still matters):

```cpp
void print(const std::vector<std::string>& tokens) { // no copy
    for (const auto& t : tokens) std::cout << t << '\n';
}
```

### Range-based for loops

The modern way to iterate a sequence — the compiler handles indexing for you:

```cpp
for (const char c : input) {
    // c is each byte of input, in order
}
```

### Character processing

C++ gives us `<cctype>` helpers (`std::isspace`, `std::islower`,
`std::tolower`, ...) — **with a famous footgun**: they take an `int` and
require a value representable as `unsigned char` (or `EOF`). A raw `char`
may be *signed* on this platform, so a byte like `0xC3` would be passed as a
*negative* `int`, which is undefined behavior. The fix is to cast:

```cpp
char c = ...;
char lower = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
```

Our design sidesteps most of this by doing **explicit ASCII classification**
(an `a`-`z`/`A`-`Z`/`0`-`9` range check) rather than relying on
locale-dependent `<cctype>` — but when we do use `std::tolower`, we cast
correctly.

### Header/source separation

Declarations go in a header (`.h`), definitions in a source file (`.cpp`).
The header is the *contract*; the source is the *implementation*.

```cpp
// tokenizer.h
#pragma once
#include <string>
#include <string_view>
#include <vector>

namespace dse {
std::vector<std::string> tokenize(std::string_view input);
}
```

```cpp
// tokenizer.cpp
#include "tokenizer.h"
// ...implementation...
```

Benefits: consumers compile faster (they only see the declaration), the
implementation can change without recompiling callers, and the header
documents the public API in one place.

### Namespaces

`dse` groups our code and prevents name collisions:

```cpp
namespace dse { ... }
// call site:
auto tokens = dse::tokenize(text);
```

### RAII (Resource Acquisition Is Initialization)

The core C++ memory-safety idiom: **resources (heap memory, files, locks)
are acquired by a constructor and released by a destructor**. `std::string`,
`std::vector`, and `std::unique_ptr` all do this — you never write
`new`/`delete` by hand, and resources are freed automatically when the object
goes out of scope, even on exceptions.

```cpp
{   // entering scope: tokens is constructed (empty)
    std::vector<std::string> tokens = dse::tokenize(text);
    ...
}   // leaving scope: tokens' destructor frees all token memory
```

### Ownership vs non-owning views

Two different ways to relate to memory:

- **Owner** — responsible for creating *and destroying* the resource.
  `std::string`, `std::vector`, `std::unique_ptr`.
- **View** — borrows; destroys nothing. `std::string_view`, `std::span`,
  raw pointers (in limited roles).

The tokenizer sits exactly on this line: it **borrows** the input
(`std::string_view` in) and **owns** the output (`std::vector<std::string>`
out). Getting this distinction right is what makes the API safe: the caller
keeps owning the input, the caller receives ownership of the output.

---

## 11. Software design concepts (mapped to our tokenizer)

- **Single Responsibility Principle (SRP)** — one reason to change: "how raw
  text becomes tokens." If we later change ranking, the tokenizer does not
  change. If we change tokenization, nothing downstream has to.
- **Separation of concerns** — the pipeline stages (tokenize → filter →
  stem → index) are distinct modules, each with a defined interface. The
  tokenizer produces tokens; it does not decide what to do with them.
- **Encapsulation** — the tokenizer hides *how* tokens are produced (its
  internal loop, classification logic) behind a function signature. Callers
  depend on the contract, not the implementation.
- **Deterministic behavior** — same input, same output, always, on every
  platform and locale. No global state, no randomness, no clock, no I/O.
  This is what makes the engine *reproducible*: the same document always
  yields the same index, and the same query always yields the same results.
- **API design** — the function signature *is* the design: minimal, honest
  about ownership (view in, owning vector out), and expressive (the name
  `tokenize` and the type say what happens).
- **Dependency direction** — the tokenizer is a *leaf*: it depends on
  nothing except the standard library. Future components (indexer, query
  processor) will depend on *it*. Keeping dependencies pointing one way
  (consumers → tokenizer) is what keeps the system modular and testable.
- **Testability** — a pure function with a defined contract is the easiest
  thing in the world to test: no mocks, no setup, no files — just call it
  with a string and compare the result.

---

## 12. Complexity

For an input of `N` characters:

- **Time: O(N).** One linear scan. Each byte is examined once, classified in
  O(1), and either appended to the current token or used as a separator.
  Lowercasing is O(1) per letter. Total work is proportional to input size —
  you cannot do better than reading every byte once.
- **Space: O(N).** The output holds at most `N` token characters (we only
  ever *drop* bytes — lowercase and separators never add characters), plus
  vector capacity overhead proportional to the number of tokens, which is at
  most `N`.

### One pass vs multiple passes

The tokenizer **must be (and is) single-pass**: because classification is
*local* (each byte's role depends only on that byte), no lookahead or
backtracking is ever needed. Each byte is touched exactly once.

Why single-pass matters:

- **Scales to large documents** — a 100 MB file costs ~100 MB of work, and
  nothing more.
- **Enables streaming later** — with a one-pass design, a future version can
  emit tokens as it scans, processing a document without ever holding it in
  memory (see Section 16).
- **Cache- and branch-friendly** — a single forward loop with simple checks
  is exactly what CPUs run fastest.

---

## 13. Edge cases

The canonical rule handles all of these; the table is the executable
specification for Phase 1B tests.

| # | Input | Expected tokens | Note |
|---|---|---|---|
| 1 | `""` | `[]` | empty input |
| 2 | `"   "` | `[]` | whitespace only |
| 3 | `"\t\n\r"` | `[]` | tabs/newlines/CR only |
| 4 | `"hello"` | `["hello"]` | one word |
| 5 | `"hello     world"` | `["hello", "world"]` | multiple spaces |
| 6 | `"\thello\tworld"` | `["hello", "world"]` | tabs as separators |
| 7 | `"hello\nworld"` | `["hello", "world"]` | newline as separator |
| 8 | `"   hello world   "` | `["hello", "world"]` | leading/trailing whitespace |
| 9 | `"hello, world!"` | `["hello", "world"]` | punctuation |
| 10 | `"!!!"` | `[]` | punctuation only |
| 11 | `"HELLO"` | `["hello"]` | uppercase |
| 12 | `"Search SEARCH search"` | `["search", "search", "search"]` | mixed case |
| 13 | `"search-engine"` | `["search", "engine"]` | hyphen splits |
| 14 | `"a--b"` | `["a", "b"]` | multiple hyphens |
| 15 | `"cat cat dog"` | `["cat", "cat", "dog"]` | duplicates preserved |
| 16 | `"42"` | `["42"]` | integer |
| 17 | `"007"` | `["007"]` | leading zeros kept |
| 18 | `"3.14"` | `["3", "14"]` | decimal splits |
| 19 | `"$9.99"` | `["9", "99"]` | currency splits |
| 20 | `"abc123"` | `["abc123"]` | mixed letters/numbers stay one token |
| 21 | `"a!!!b"` | `["a", "b"]` | consecutive punctuation |
| 22 | `"don't"` | `["don", "t"]` | apostrophe splits |
| 23 | `"under_score"` | `["under", "score"]` | underscore splits |
| 24 | `"and/or"` | `["and", "or"]` | slash splits |
| 25 | `"C++"` | `["c"]` | plus signs are separators |
| 26 | `"rock&roll"` | `["rock", "roll"]` | ampersand splits |
| 27 | `"user@example.com"` | `["user", "example", "com"]` | email fragments |
| 28 | `"https://example.com/path?q=1"` | `["https", "example", "com", "path", "q", "1"]` | URL fragments |
| 29 | `"café"` | `["caf"]` | non-ASCII byte drops the rest (documented limitation) |
| 30 | *(very long input, e.g. 100 KB of words)* | same words, in order, no crashes | long input, O(N) |

---

## 14. Testing strategy (designed before implementation)

Phase 1B will write tests **first**, grouped by the property each group
proves. Each group is one or more `TEST(...)` cases in a single
`tokenizer_test.cpp`.

| Group | What it proves | Example cases |
|---|---|---|
| Basic tokenization | the core contract: text in, ordered tokens out | `"hello world" -> ["hello","world"]`; single word; empty input |
| Normalization | case-insensitive vocabulary | `"Search SEARCH search" -> ["search","search","search"]`; `"HELLO" -> ["hello"]` |
| Punctuation | separators remove punctuation without losing words | `"hello, world!"`; `"a!!!b"`; `"..."` |
| Whitespace | any run of whitespace = one break | multiple spaces; tabs; newlines; leading/trailing |
| Hyphen handling | hyphens split into separate tokens | `"search-engine"`; `"a--b"` |
| Duplicates | term-frequency information is preserved | `"cat cat dog" -> ["cat","cat","dog"]` |
| Numbers/symbols | documented ASCII policy for non-letters | integers; decimals; signs; email; URL; `C++` |
| Edge cases | robustness: nothing crashes, nothing dangles | empty; whitespace-only; punctuation-only; long input; non-ASCII bytes |

Every assertion compares the **entire returned vector** to the expected
vector (`EXPECT_EQ(tokens, std::vector<std::string>{...})`) — exact match on
content *and* order. That single style of assertion, applied to the table in
Section 13, gives us a thorough spec-as-tests for a pure function.

---

## 15. Design trade-offs

- **Simplicity vs linguistic sophistication.** A maximal-run rule is
  trivially correct and testable; a linguistically "smart" tokenizer is
  complex, slower to build, and harder to verify. Phase 1 chooses simplicity;
  sophistication is added later in separate stages where it can be measured.
- **ASCII vs Unicode.** Full Unicode needs decoding, normalization, and
  per-script rules (Section 6). Phase 1 does ASCII only, and *documents* the
  limitation instead of pretending otherwise.
- **One-pass vs multiple passes.** One pass: O(N), streaming-friendly, simple.
  Multiple passes would buy nothing here.
- **Owning strings vs string views in the output.** Owning strings: safe
  lifetime, correct for lowercasing, trivial to test. Views: slightly
  cheaper, but fragile and unable to hold the transformed text. Owning wins.
- **Tokenizer responsibility vs separate preprocessing stages.** Stop words,
  stemming, etc. are downstream filters. Keeping them out of the tokenizer
  makes each piece testable and makes the pipeline configurable.
- **Correctness vs performance.** At this phase, correctness and
  determinism are the priority; the single-pass design already gives
  acceptable performance, and benchmarking is a later phase. Premature
  micro-optimization (arenas, custom allocators) is explicitly deferred.

### Recommendation

The canonical rule in Section 4, implemented as the pure function
`dse::tokenize(std::string_view) -> std::vector<std::string>`, with a
single-pass loop. It is the smallest design that satisfies the contract,
maximizes testability, and leaves every door open for future phases.

---

## 16. Future extensions (without breaking the architecture)

The initial API deliberately stays *minimal* so these can be added later as
**additive** changes (new functions, new options) rather than breaking ones:

- **Unicode support** — replace the ASCII classification inside the same
  stage with UTF-8 decoding + code-point categories. The contract (text in,
  tokens out) is unchanged; only the rule table changes.
- **Language-specific tokenization** — e.g., CJK segmentation. A new
  tokenizer variant selected by configuration, not a change to the contract.
- **Configurable stop-word filtering** — a *separate* stage after
  tokenization, parameterized by a stop-word set.
- **Stemming / lemmatization** — separate downstream stages (Section 8).
- **Synonym expansion, spell correction** — query-side stages; the
  tokenizer's output feeds them unchanged.
- **Token metadata (positions, offsets)** — when phrase queries arrive, add
  a *new* function (e.g., `tokenize_with_positions`) returning
  `(token, offset)` pairs, leaving `tokenize` untouched for the many callers
  that only need the plain list.
- **Streaming** — because the core scan is single-pass, a future
  `tokenize_stream(input, sink)` can emit tokens without building the full
  vector, enabling documents larger than memory.

The reason the initial API avoids coupling to these: every future feature
above is **optional**. If `tokenize` already returned positions or
language-tagged tokens, every Phase 1 caller would pay the cost and inherit
the complexity of features nobody asked for yet. Small, honest contracts
make big systems evolvable.

---

## 17. The Phase 1 contract (summary)

- **Responsibility:** raw text → normalized token list. Nothing else.
- **Rule:** tokens are maximal runs of `[A-Za-z0-9]`; all other bytes are
  separators; ASCII uppercase is lowercased; order and duplicates are
  preserved; empty runs vanish.
- **API (proposal):** `std::vector<std::string> dse::tokenize(std::string_view input);`
- **Complexity:** O(N) time, O(N) space, single pass.
- **Determinism:** pure function; identical input always yields identical
  output; no locale or platform dependence.
- **Known limitations:** ASCII-only; decimals, contractions, emails, and URLs
  split; stop words, stemming, and lemmatization are out of scope by design.

Phase 1B implemented exactly this contract — see the Implementation Results
section below for the actual, verified outcome.

---

## Implementation Results — Phase 1B

This section documents the ACTUAL implementation: Phase 1B-1 (core library
structure) and Phase 1B-2 (tokenizer algorithm and tests), as built and
verified on this machine with the MSYS2 UCRT64 toolchain (GCC 16.2.0,
CMake 4.4.2, Ninja 1.13.2, C++20). It is a record of what exists, not a
proposal.

### 1. Final API

Exactly as proposed in Phase 1A and frozen in Phase 1B-1:

```cpp
namespace dse {
std::vector<std::string> tokenize(std::string_view input);
}
```

The public header (`src/tokenizer.h`) did not change during implementation.

### 2. Final implementation structure

`src/tokenizer.cpp` contains:

- `is_token_char(unsigned char)` — explicit ASCII range check for `A-Z`,
  `a-z`, `0-9`;
- `to_ascii_lower(unsigned char)` — explicit `'A'-'Z' -> 'a'-'z'` via
  `c + ('a' - 'A')`; every other byte returned unchanged;
- a single `for` loop scanning the input once, left to right;
- a `current` token buffer (`std::string`) reused across flushes;
- flush behavior: on a separator byte, a non-empty `current` is moved into
  the result vector and cleared;
- a final trailing-token flush after the loop.

The function has no state, performs no I/O, and never stores the input view.

### 3. Why explicit ASCII range checks instead of `std::isalnum` / `std::tolower`

The original implementation used `std::tolower` (with the mandatory
`unsigned char` cast). A code review replaced it with explicit range checks:

- **Locale dependence.** `std::isalnum`, `std::isspace` and `std::tolower`
  behavior depends on the C locale; results could differ between machines or
  environments. Explicit `'A'..'Z'` range checks are identical everywhere,
  which is exactly what a deterministic contract requires.
- **The `<cctype>` footgun.** Those functions take an `int` and require a
  value representable as `unsigned char` (or `EOF`); a raw `char` may be
  signed, making bytes ≥ `0x80` undefined behavior without a cast.
  Range checks have no such requirement.
- **Self-documenting.** The contract is explicitly ASCII-only, so the range
  checks say in code exactly what the contract says in words.

For the ASCII contract, both approaches produce identical output; the range
checks are stronger (no locale, no ctype dependency) and were adopted.

### 4. Ownership model

- **Input** `std::string_view` — read-only and never stored. The caller's
  buffer only needs to outlive the call, so passing a temporary
  `std::string` is safe (verified by a dedicated test).
- **Output** `std::vector<std::string>` — owns every token string; the
  caller controls lifetime and mutability.

### 5. Actual complexity

- **Time: O(N)** — one pass over the input; each byte is classified and
  handled in O(1).
- **Output space: O(N)** — total token characters never exceed the input
  length (only bytes are dropped), plus vector capacity proportional to the
  number of tokens.

### 6. Final project structure

```
src/
    tokenizer.h      public API (the contract)
    tokenizer.cpp    implementation
    main.cpp         application entry point (unchanged from Phase 0)
tests/
    tokenizer_test.cpp   45 tests across the eight Phase 1A test groups
    smoke_test.cpp       Phase 0 smoke tests (2)
```

The tokenizer lives in the reusable `dse_core` static library.

### 7. CMake dependency graph

```
dse_core (STATIC library)          src/tokenizer.cpp; include src/ PUBLIC
  ^                                   no external dependencies
  |
  |-- DistributedSearchEngine (executable)   src/main.cpp
  |        links dse_core PRIVATE
  |
  `-- tokenizer_test (executable)           tests/tokenizer_test.cpp
           links GTest::gtest_main + dse_core

smoke_test (executable)            tests/smoke_test.cpp
           links GTest::gtest_main (unchanged Phase 0 target)

GoogleTest v1.18.0 (FetchContent)  provides gtest / gtest_main
```

### 8. Actual testing results

- 45 tokenizer tests (basic, normalization, punctuation, whitespace, hyphen,
  duplicates, numbers/symbols, edge cases);
- 2 Phase 0 smoke tests;
- **47/47 tests passed (100%)**, zero failures;
- total test execution time ≈ **1.59 s** on this machine — a functional
  check, not a benchmark;
- build completed successfully with **zero warnings**
  (`-Wall -Wextra -Wpedantic`).

### 9. Actual application verification

`./build/DistributedSearchEngine.exe` ran successfully: it printed the
application/phase banner and `Built with C++ standard: 202002`
(`__cplusplus == 202002`), with exit code 0.

### 10. Design vs Implementation Review

| Phase 1A decision | Phase 1B outcome |
|---|---|
| Free function `dse::tokenize`, no class, no options | Implemented exactly as designed |
| Canonical rule: maximal runs of `[A-Za-z0-9]`, everything else a separator | Implemented exactly (`is_token_char`) |
| Digits stay inside tokens; hyphens split; duplicates and order preserved | Implemented exactly, verified by tests |
| ASCII-only; non-ASCII bytes are separators | Implemented exactly |
| Single-pass O(N) scan | Implemented exactly |
| Lowercase via `std::tolower` + `unsigned char` cast (ADR-001 wording) | Behavior identical; mechanism refined to explicit range checks (`to_ascii_lower`) after code review |
| Owning `std::vector<std::string>` output; `std::string_view` input never stored | Implemented exactly, lifetime verified by test |
| Core library shared by app and tests | Implemented (`dse_core` static library) |

Every behavioral decision from Phase 1A was implemented; the only change was
the internal lowercase mechanism, which produces identical output for the
ASCII contract with stronger determinism.

### 11. Lessons Learned

- **Header/source separation.** The header froze the contract early; the
  implementation changed twice (stub → `std::tolower` version → range-check
  version) without any consumer, test, or CMake change.
- **Reusable static library.** The app and tests share one compiled
  tokenizer; the test binary exercises the exact artifact the app uses.
- **PUBLIC vs PRIVATE.** Include directory PUBLIC (consumers need the
  header), warnings PRIVATE (an implementation detail). Getting this right
  up front avoided CMake churn.
- **`string_view` lifetime.** A temporary input is safe because the view is
  never stored — verified with a dedicated test rather than assumed.
- **ASCII classification.** Explicit range checks made the code obviously
  locale-independent and removed the `<cctype>` footgun.
- **Deterministic processing.** The pure-function design made all 45 tests
  trivial: input → expected vector.
- **Unit testing before integration.** The test target existed (as a
  placeholder) before the algorithm; replacing the stub was verified in a
  single build+test run.
