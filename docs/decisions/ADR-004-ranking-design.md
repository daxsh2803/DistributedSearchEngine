# ADR-004: TF-IDF Ranking Design

## Status

Accepted

## Context

Phases 1–3 provide Boolean retrieval: a query returns the set of matching
documents, with no notion of relevance. Phase 4 adds ranking so that
results are ordered by estimated relevance.

The ranking layer must:
- Compose cleanly with the existing tokenizer (Phase 1), inverted index
  (Phase 2), and query processor (Phase 3)
- Produce deterministic, reproducible scores
- Be testable in isolation
- Not modify the existing Phase 1–3 APIs

## Decision

Implement TF-IDF (Term Frequency × Inverse Document Frequency) scoring
via a new `dse::Ranker` class.

### Data Model

```cpp
struct RankedResult {
    doc_id document_id;
    double score;

    friend bool operator==(const RankedResult&, const RankedResult&) = default;
};
```

### API

```cpp
class Ranker {
public:
    explicit Ranker(const InvertedIndex& index);
    std::vector<RankedResult> ranked_and(std::string_view query) const;
    std::vector<RankedResult> ranked_or(std::string_view query) const;
};
```

### Scoring Formula

```
tfidf(t, d) = tf(t, d) × idf(t)
idf(t) = ln(N / df(t))
score(d, q) = Σ tfidf(t, d) for each distinct query term t
```

Where:
- tf(t, d) = `Posting.term_frequency` (from Phase 2)
- df(t) = `index.postings(t).size()`
- N = `index.document_count()`

### Ranking Rules

1. Tokenize the query using `dse::tokenize` (Phase 1)
2. Deduplicate query terms (sort + unique)
3. For each distinct term, look up postings in the index
4. Compute IDF = ln(N / df)
5. For each posting, accumulate tf × idf into a per-document score
6. For AND: only documents appearing in ALL term posting lists are scored
7. For OR: all documents appearing in ANY term posting list are scored
8. Results sorted by score descending
9. Tie-breaking: by doc_id ascending (deterministic)

### Lifetime

The Ranker borrows the InvertedIndex (non-owning pointer, same pattern as
QueryProcessor). The index must outlive the ranker.

## Alternatives Considered

1. **Extend QueryProcessor with ranking** — rejected: violates Single
   Responsibility; mixes Boolean logic with scoring

2. **Free function `rank(const InvertedIndex&, string_view)`** — rejected:
   would re-tokenize and re-lookup on every call; a class allows caching
   if needed later

3. **BM25 from the start** — rejected: TF-IDF is simpler, well-understood,
   and the necessary foundation for understanding BM25's improvements

4. **Use log₂ instead of ln** — considered equivalent (same ordering);
   ln chosen for consistency with information theory literature

## Trade-offs

- Raw TF (not log-saturated) is used for simplicity. This means very high
  term frequencies dominate scores disproportionately. BM25 will fix this.
- No document-length normalization. Longer documents accumulate higher TF
  values naturally. BM25 addresses this.
- IDF uses natural log. Other bases (2, 10) would change the scale but not
  the ranking order.

## Consequences

- New files: `src/ranker.h`, `src/ranker.cpp`, `tests/ranker_test.cpp`
- Modified: `CMakeLists.txt` (add ranker.cpp to dse_core, add ranker_test)
- No changes to Phase 1–3 code
- The `RankedResult` struct can be extended later (e.g., adding explanation
  fields, snippet data) without breaking the API

## Future Evolution

1. **BM25**: replace `tf × idf` with BM25's saturated TF and length normalization
2. **Field weighting**: score different fields (title, body) with different weights
3. **Query expansion**: add synonyms before scoring
4. **Learning-to-rank**: replace TF-IDF with an ML model trained on click data
5. **Score explanation**: return per-term score breakdown for debugging
