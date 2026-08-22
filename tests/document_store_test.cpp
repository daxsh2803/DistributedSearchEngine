// Distributed Search Engine - Document Store tests (Phase 6B-1, 7A-1).
//
// Tests for dse::DocumentStore covering: basic add/get, contains,
// missing documents, size tracking, multiple documents, duplicate
// ID rejection, duplicate does not overwrite, doc_id 0, empty content,
// long content, deterministic behavior, and JSONL persistence.

#include <gtest/gtest.h>

#include "document_store.h"

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <string>
#include <string_view>

namespace {

using dse::Document;
using dse::DocumentStore;

// Helper: create a unique temporary file path for tests.
std::string temp_path(const std::string& name)
{
    return std::tmpnam(nullptr) + std::string("_") + name + ".jsonl";
}

// Helper: read entire file as a string.
std::string read_file(const std::string& path)
{
    std::ifstream ifs(path);
    return std::string(std::istreambuf_iterator<char>(ifs),
                       std::istreambuf_iterator<char>());
}

// RAII helper: removes a file on destruction.
struct TempFile {
    std::string path;
    ~TempFile() { std::remove(path.c_str()); }
};

} // namespace

// ---------------------------------------------------------------------------
// 1. Add and retrieve a document
// ---------------------------------------------------------------------------

TEST(DocumentStoreBasic, AddAndRetrieve)
{
    DocumentStore store;
    EXPECT_TRUE(store.add({1, "hello world"}));

    const auto doc = store.get(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->id, 1u);
    EXPECT_EQ(doc->content, "hello world");
}

TEST(DocumentStoreBasic, AddReturnsTrue)
{
    DocumentStore store;
    EXPECT_TRUE(store.add({42, "some content"}));
}

// ---------------------------------------------------------------------------
// 2. Contains existing document
// ---------------------------------------------------------------------------

TEST(DocumentStoreContains, ExistingDocumentReturnsTrue)
{
    DocumentStore store;
    store.add({10, "content"});

    EXPECT_TRUE(store.contains(10));
}

// ---------------------------------------------------------------------------
// 3. Contains missing document
// ---------------------------------------------------------------------------

TEST(DocumentStoreContains, MissingDocumentReturnsFalse)
{
    DocumentStore store;
    store.add({1, "content"});

    EXPECT_FALSE(store.contains(2));
    EXPECT_FALSE(store.contains(999));
}

// ---------------------------------------------------------------------------
// 4. Get missing document returns empty optional
// ---------------------------------------------------------------------------

TEST(DocumentStoreGet, MissingDocumentReturnsNullopt)
{
    DocumentStore store;
    store.add({1, "content"});

    EXPECT_FALSE(store.get(2).has_value());
    EXPECT_FALSE(store.get(0).has_value());
    EXPECT_FALSE(store.get(UINT32_MAX).has_value());
}

// ---------------------------------------------------------------------------
// 5. Size
// ---------------------------------------------------------------------------

TEST(DocumentStoreSize, EmptyStore)
{
    DocumentStore store;
    EXPECT_EQ(store.size(), 0u);
}

TEST(DocumentStoreSize, AfterAddingDocuments)
{
    DocumentStore store;
    store.add({1, "a"});
    EXPECT_EQ(store.size(), 1u);

    store.add({2, "b"});
    EXPECT_EQ(store.size(), 2u);

    store.add({3, "c"});
    EXPECT_EQ(store.size(), 3u);
}

// ---------------------------------------------------------------------------
// 6. Multiple documents
// ---------------------------------------------------------------------------

TEST(DocumentStoreMultiple, DistinctIdsAllRetrievable)
{
    DocumentStore store;
    store.add({1, "first"});
    store.add({2, "second"});
    store.add({3, "third"});

    EXPECT_EQ(store.size(), 3u);

    const auto d1 = store.get(1);
    ASSERT_TRUE(d1.has_value());
    EXPECT_EQ(d1->content, "first");

    const auto d2 = store.get(2);
    ASSERT_TRUE(d2.has_value());
    EXPECT_EQ(d2->content, "second");

    const auto d3 = store.get(3);
    ASSERT_TRUE(d3.has_value());
    EXPECT_EQ(d3->content, "third");
}

// ---------------------------------------------------------------------------
// 7. Duplicate ID rejected
// ---------------------------------------------------------------------------

TEST(DocumentStoreDuplicate, AddDuplicateReturnsFalse)
{
    DocumentStore store;
    EXPECT_TRUE(store.add({1, "first"}));
    EXPECT_FALSE(store.add({1, "second"}));
}

// ---------------------------------------------------------------------------
// 8. Duplicate does not overwrite existing content
// ---------------------------------------------------------------------------

TEST(DocumentStoreDuplicate, ExistingContentUnchanged)
{
    DocumentStore store;
    store.add({42, "original"});
    store.add({42, "overwrite attempt"});

    const auto doc = store.get(42);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "original");
}

TEST(DocumentStoreDuplicate, SizeUnchangedAfterDuplicate)
{
    DocumentStore store;
    store.add({1, "a"});
    store.add({1, "b"});
    store.add({1, "c"});

    EXPECT_EQ(store.size(), 1u);
}

// ---------------------------------------------------------------------------
// 9. doc_id 0
// ---------------------------------------------------------------------------

TEST(DocumentStoreEdgeCases, DocIdZeroIsValid)
{
    DocumentStore store;
    EXPECT_TRUE(store.add({0, "zero"}));

    const auto doc = store.get(0);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->id, 0u);
    EXPECT_EQ(doc->content, "zero");
    EXPECT_TRUE(store.contains(0));
}

// ---------------------------------------------------------------------------
// 10. Empty content can be stored
// ---------------------------------------------------------------------------

TEST(DocumentStoreEdgeCases, EmptyContentStored)
{
    DocumentStore store;
    EXPECT_TRUE(store.add({1, ""}));

    const auto doc = store.get(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "");
}

// ---------------------------------------------------------------------------
// 11. Long content
// ---------------------------------------------------------------------------

TEST(DocumentStoreEdgeCases, LongContentStored)
{
    // 10,000 characters
    std::string long_content(10'000, 'x');

    DocumentStore store;
    EXPECT_TRUE(store.add({1, long_content}));

    const auto doc = store.get(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, long_content);
    EXPECT_EQ(doc->content.size(), 10'000u);
}

// ---------------------------------------------------------------------------
// 12. Deterministic behavior
// ---------------------------------------------------------------------------

TEST(DocumentStoreDeterminism, SameInsertionsSameState)
{
    auto make_store = []() {
        DocumentStore store;
        store.add({1, "alpha"});
        store.add({2, "beta"});
        store.add({3, "gamma"});
        return store;
    };

    DocumentStore a = make_store();
    DocumentStore b = make_store();

    EXPECT_EQ(a.size(), b.size());
    EXPECT_EQ(a.get(1), b.get(1));
    EXPECT_EQ(a.get(2), b.get(2));
    EXPECT_EQ(a.get(3), b.get(3));
}

// ---------------------------------------------------------------------------
// Additional: max doc_id
// ---------------------------------------------------------------------------

TEST(DocumentStoreEdgeCases, MaxDocIdIsValid)
{
    constexpr dse::doc_id kMax = UINT32_MAX;
    DocumentStore store;
    EXPECT_TRUE(store.add({kMax, "max"}));

    const auto doc = store.get(kMax);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->id, kMax);
    EXPECT_EQ(doc->content, "max");
}

// ---------------------------------------------------------------------------
// Additional: non-contiguous IDs
// ---------------------------------------------------------------------------

TEST(DocumentStoreMultiple, NonContiguousIdsWork)
{
    DocumentStore store;
    store.add({5, "five"});
    store.add({100, "hundred"});
    store.add({1, "one"});
    store.add({999, "nine-nine-nine"});

    EXPECT_EQ(store.size(), 4u);
    EXPECT_TRUE(store.contains(5));
    EXPECT_TRUE(store.contains(100));
    EXPECT_TRUE(store.contains(1));
    EXPECT_TRUE(store.contains(999));
    EXPECT_FALSE(store.contains(6));
    EXPECT_FALSE(store.contains(50));
}

// ---------------------------------------------------------------------------
// Additional: content with special characters
// ---------------------------------------------------------------------------

TEST(DocumentStoreEdgeCases, ContentWithSpecialCharacters)
{
    DocumentStore store;
    const std::string content = "Hello, world! Line1\nLine2\tTabbed \"quoted\" 'single'";
    store.add({1, content});

    const auto doc = store.get(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, content);
}

// ===========================================================================
// Phase 7A-1: JSONL Persistence Tests
// ===========================================================================

// ---------------------------------------------------------------------------
// 20. Save/load round trip
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, SaveLoadRoundTrip)
{
    const auto path = temp_path("roundtrip");
    TempFile guard{path};

    DocumentStore store;
    store.add({1, "hello world"});
    store.add({2, "foo bar"});

    EXPECT_TRUE(store.save(path));

    DocumentStore loaded;
    EXPECT_TRUE(loaded.load(path));
    EXPECT_EQ(loaded.size(), 2u);

    const auto d1 = loaded.get(1);
    ASSERT_TRUE(d1.has_value());
    EXPECT_EQ(d1->content, "hello world");

    const auto d2 = loaded.get(2);
    ASSERT_TRUE(d2.has_value());
    EXPECT_EQ(d2->content, "foo bar");
}

// ---------------------------------------------------------------------------
// 21. Multiple documents
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, MultipleDocuments)
{
    const auto path = temp_path("multi");
    TempFile guard{path};

    DocumentStore store;
    store.add({1, "alpha"});
    store.add({2, "beta"});
    store.add({3, "gamma"});
    store.add({100, "delta"});

    EXPECT_TRUE(store.save(path));

    DocumentStore loaded;
    EXPECT_TRUE(loaded.load(path));
    EXPECT_EQ(loaded.size(), 4u);
    EXPECT_EQ(loaded.get(1)->content, "alpha");
    EXPECT_EQ(loaded.get(2)->content, "beta");
    EXPECT_EQ(loaded.get(3)->content, "gamma");
    EXPECT_EQ(loaded.get(100)->content, "delta");
}

// ---------------------------------------------------------------------------
// 22. doc_id = 0
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, DocIdZero)
{
    const auto path = temp_path("zero");
    TempFile guard{path};

    DocumentStore store;
    store.add({0, "zero doc"});
    store.add({1, "one doc"});

    EXPECT_TRUE(store.save(path));

    DocumentStore loaded;
    EXPECT_TRUE(loaded.load(path));
    EXPECT_EQ(loaded.size(), 2u);

    const auto d0 = loaded.get(0);
    ASSERT_TRUE(d0.has_value());
    EXPECT_EQ(d0->content, "zero doc");
}

// ---------------------------------------------------------------------------
// 23. Non-contiguous IDs
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, NonContiguousIds)
{
    const auto path = temp_path("noncontig");
    TempFile guard{path};

    DocumentStore store;
    store.add({5, "five"});
    store.add({100, "hundred"});
    store.add({1, "one"});
    store.add({999, "nine-nine-nine"});

    EXPECT_TRUE(store.save(path));

    DocumentStore loaded;
    EXPECT_TRUE(loaded.load(path));
    EXPECT_EQ(loaded.size(), 4u);
    EXPECT_TRUE(loaded.contains(5));
    EXPECT_TRUE(loaded.contains(100));
    EXPECT_TRUE(loaded.contains(1));
    EXPECT_TRUE(loaded.contains(999));
}

// ---------------------------------------------------------------------------
// 24. Empty content
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, EmptyContent)
{
    const auto path = temp_path("emptycontent");
    TempFile guard{path};

    DocumentStore store;
    store.add({1, ""});
    store.add({2, "not empty"});

    EXPECT_TRUE(store.save(path));

    DocumentStore loaded;
    EXPECT_TRUE(loaded.load(path));
    EXPECT_EQ(loaded.size(), 2u);
    EXPECT_EQ(loaded.get(1)->content, "");
    EXPECT_EQ(loaded.get(2)->content, "not empty");
}

// ---------------------------------------------------------------------------
// 25. Special characters / JSON escaping
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, SpecialCharactersJsonEscaping)
{
    const auto path = temp_path("escaping");
    TempFile guard{path};

    const std::string content =
        "Line1\nLine2\tTabbed \"quoted\" 'single' back\\slash";

    DocumentStore store;
    store.add({1, content});

    EXPECT_TRUE(store.save(path));

    DocumentStore loaded;
    EXPECT_TRUE(loaded.load(path));
    const auto doc = loaded.get(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, content);
}

// ---------------------------------------------------------------------------
// 26. Long content
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, LongContent)
{
    const auto path = temp_path("longcontent");
    TempFile guard{path};

    std::string long_content(50'000, 'x');

    DocumentStore store;
    store.add({1, long_content});

    EXPECT_TRUE(store.save(path));

    DocumentStore loaded;
    EXPECT_TRUE(loaded.load(path));
    const auto doc = loaded.get(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content.size(), 50'000u);
    EXPECT_EQ(doc->content, long_content);
}

// ---------------------------------------------------------------------------
// 27. Save creates file
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, SaveCreatesFile)
{
    const auto path = temp_path("createsfile");
    TempFile guard{path};

    DocumentStore store;
    store.add({1, "test"});

    EXPECT_TRUE(store.save(path));

    // Verify file exists and is non-empty.
    std::ifstream ifs(path);
    EXPECT_TRUE(ifs.is_open());
    std::string file_content((std::istreambuf_iterator<char>(ifs)),
                             std::istreambuf_iterator<char>());
    EXPECT_FALSE(file_content.empty());
}

// ---------------------------------------------------------------------------
// 28. Save overwrites existing file
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, SaveOverwritesExistingFile)
{
    const auto path = temp_path("overwrite");
    TempFile guard{path};

    // Write initial content.
    {
        std::ofstream ofs(path);
        ofs << "old content that should be replaced";
    }

    DocumentStore store;
    store.add({1, "new"});
    EXPECT_TRUE(store.save(path));

    // File should contain only the new JSONL, not the old content.
    const auto content = read_file(path);
    EXPECT_EQ(content.find("old content"), std::string::npos);
    EXPECT_NE(content.find("new"), std::string::npos);
}

// ---------------------------------------------------------------------------
// 29. Missing file (load returns false, store unchanged)
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, MissingFileReturnsFalse)
{
    DocumentStore store;
    store.add({1, "existing"});

    // Try to load from a file that does not exist.
    EXPECT_FALSE(store.load("/nonexistent/path/to/file.jsonl"));

    // Existing store must be unchanged.
    EXPECT_EQ(store.size(), 1u);
    const auto doc = store.get(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "existing");
}

// ---------------------------------------------------------------------------
// 30. Malformed JSON line
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, MalformedJsonLineSkipped)
{
    const auto path = temp_path("malformed");
    TempFile guard{path};

    // Write a file with a mix of valid and invalid lines.
    {
        std::ofstream ofs(path);
        ofs << "{\"id\": 1, \"content\": \"good\"}\n";
        ofs << "this is not json\n";
        ofs << "{\"id\": 2, \"content\": \"also good\"}\n";
    }

    DocumentStore store;
    EXPECT_TRUE(store.load(path));  // returns true (file opened)
    EXPECT_EQ(store.size(), 2u);    // only valid lines loaded
    EXPECT_EQ(store.get(1)->content, "good");
    EXPECT_EQ(store.get(2)->content, "also good");
}

// ---------------------------------------------------------------------------
// 31. Missing id field
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, MissingIdFieldSkipped)
{
    const auto path = temp_path("noid");
    TempFile guard{path};

    {
        std::ofstream ofs(path);
        ofs << "{\"content\": \"no id\"}\n";
        ofs << "{\"id\": 1, \"content\": \"valid\"}\n";
    }

    DocumentStore store;
    EXPECT_TRUE(store.load(path));
    EXPECT_EQ(store.size(), 1u);
    EXPECT_EQ(store.get(1)->content, "valid");
}

// ---------------------------------------------------------------------------
// 32. Missing content field
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, MissingContentFieldSkipped)
{
    const auto path = temp_path("nocontent");
    TempFile guard{path};

    {
        std::ofstream ofs(path);
        ofs << "{\"id\": 1}\n";
        ofs << "{\"id\": 2, \"content\": \"valid\"}\n";
    }

    DocumentStore store;
    EXPECT_TRUE(store.load(path));
    EXPECT_EQ(store.size(), 1u);
    EXPECT_EQ(store.get(2)->content, "valid");
}

// ---------------------------------------------------------------------------
// 33. Wrong id type (string instead of unsigned int)
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, WrongIdTypeSkipped)
{
    const auto path = temp_path("wrongidtype");
    TempFile guard{path};

    {
        std::ofstream ofs(path);
        ofs << "{\"id\": \"not a number\", \"content\": \"bad\"}\n";
        ofs << "{\"id\": 1, \"content\": \"valid\"}\n";
    }

    DocumentStore store;
    EXPECT_TRUE(store.load(path));
    EXPECT_EQ(store.size(), 1u);
    EXPECT_EQ(store.get(1)->content, "valid");
}

// ---------------------------------------------------------------------------
// 34. Wrong content type (int instead of string)
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, WrongContentTypeSkipped)
{
    const auto path = temp_path("wrongcontenttype");
    TempFile guard{path};

    {
        std::ofstream ofs(path);
        ofs << "{\"id\": 1, \"content\": 12345}\n";
        ofs << "{\"id\": 2, \"content\": \"valid\"}\n";
    }

    DocumentStore store;
    EXPECT_TRUE(store.load(path));
    EXPECT_EQ(store.size(), 1u);
    EXPECT_EQ(store.get(2)->content, "valid");
}

// ---------------------------------------------------------------------------
// 35. Duplicate IDs in persisted file (first wins)
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, DuplicateIdsInFileFirstWins)
{
    const auto path = temp_path("dupes");
    TempFile guard{path};

    {
        std::ofstream ofs(path);
        ofs << "{\"id\": 1, \"content\": \"first\"}\n";
        ofs << "{\"id\": 1, \"content\": \"second\"}\n";
    }

    DocumentStore store;
    EXPECT_TRUE(store.load(path));
    EXPECT_EQ(store.size(), 1u);
    EXPECT_EQ(store.get(1)->content, "first");
}

// ---------------------------------------------------------------------------
// 36. Deterministic save ordering (sorted by doc_id)
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, DeterministicSaveOrdering)
{
    // Insert in reverse order, then verify file is sorted by doc_id.
    const auto path = temp_path("order");
    TempFile guard{path};

    DocumentStore store;
    store.add({100, "hundred"});
    store.add({1, "one"});
    store.add({50, "fifty"});
    store.add({10, "ten"});

    EXPECT_TRUE(store.save(path));

    const auto content = read_file(path);

    // nlohmann::json::dump() produces compact JSON (no spaces after colons).
    // Find positions of each ID in the file.
    const auto pos1 = content.find("\"id\":1");
    const auto pos10 = content.find("\"id\":10");
    const auto pos50 = content.find("\"id\":50");
    const auto pos100 = content.find("\"id\":100");

    EXPECT_NE(pos1, std::string::npos);
    EXPECT_NE(pos10, std::string::npos);
    EXPECT_NE(pos50, std::string::npos);
    EXPECT_NE(pos100, std::string::npos);

    EXPECT_LT(pos1, pos10);
    EXPECT_LT(pos10, pos50);
    EXPECT_LT(pos50, pos100);
}

// ---------------------------------------------------------------------------
// 37. all() exposes complete document set
// ---------------------------------------------------------------------------

TEST(DocumentStoreAll, AllReturnsCompleteMap)
{
    DocumentStore store;
    store.add({1, "alpha"});
    store.add({2, "beta"});
    store.add({3, "gamma"});

    const auto& docs = store.all();
    EXPECT_EQ(docs.size(), 3u);
    EXPECT_TRUE(docs.contains(1));
    EXPECT_TRUE(docs.contains(2));
    EXPECT_TRUE(docs.contains(3));
    EXPECT_EQ(docs.at(1).content, "alpha");
    EXPECT_EQ(docs.at(2).content, "beta");
    EXPECT_EQ(docs.at(3).content, "gamma");
}

TEST(DocumentStoreAll, EmptyStoreAllReturnsEmptyMap)
{
    DocumentStore store;
    const auto& docs = store.all();
    EXPECT_TRUE(docs.empty());
}

// ---------------------------------------------------------------------------
// 38. Existing store remains unchanged when load cannot open file
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, ExistingStoreUnchangedOnLoadFailure)
{
    DocumentStore store;
    store.add({1, "original"});
    store.add({2, "preserved"});

    EXPECT_FALSE(store.load("/definitely/does/not/exist.jsonl"));

    EXPECT_EQ(store.size(), 2u);
    EXPECT_EQ(store.get(1)->content, "original");
    EXPECT_EQ(store.get(2)->content, "preserved");
}

// ---------------------------------------------------------------------------
// 39. Mixed valid + malformed records
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, MixedValidAndMalformedRecords)
{
    const auto path = temp_path("mixed");
    TempFile guard{path};

    {
        std::ofstream ofs(path);
        ofs << "{\"id\": 1, \"content\": \"good1\"}\n";
        ofs << "not json at all\n";
        ofs << "{\"id\": 2, \"content\": \"good2\"}\n";
        ofs << "{}\n";  // empty object, missing both fields
        ofs << "{\"id\": 3, \"content\": \"good3\"}\n";
        ofs << "[1, 2, 3]\n";  // array, not object
    }

    DocumentStore store;
    EXPECT_TRUE(store.load(path));
    EXPECT_EQ(store.size(), 3u);
    EXPECT_EQ(store.get(1)->content, "good1");
    EXPECT_EQ(store.get(2)->content, "good2");
    EXPECT_EQ(store.get(3)->content, "good3");
}

// ---------------------------------------------------------------------------
// 40. Negative id (signed) is rejected
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, NegativeIdTypeSkipped)
{
    const auto path = temp_path("negid");
    TempFile guard{path};

    {
        std::ofstream ofs(path);
        ofs << "{\"id\": -1, \"content\": \"negative\"}\n";
        ofs << "{\"id\": 1, \"content\": \"valid\"}\n";
    }

    DocumentStore store;
    EXPECT_TRUE(store.load(path));
    EXPECT_EQ(store.size(), 1u);
    EXPECT_EQ(store.get(1)->content, "valid");
}

// ---------------------------------------------------------------------------
// 41. Save/load preserves content with newlines
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, ContentWithNewlines)
{
    const auto path = temp_path("newlines");
    TempFile guard{path};

    const std::string content = "line1\nline2\nline3";

    DocumentStore store;
    store.add({1, content});

    EXPECT_TRUE(store.save(path));

    DocumentStore loaded;
    EXPECT_TRUE(loaded.load(path));
    EXPECT_EQ(loaded.get(1)->content, content);
}

// ---------------------------------------------------------------------------
// 42. Save/load empty store (empty file)
// ---------------------------------------------------------------------------

TEST(DocumentStorePersistence, EmptyStoreSaveLoad)
{
    const auto path = temp_path("emptystore");
    TempFile guard{path};

    DocumentStore store;
    EXPECT_TRUE(store.save(path));

    DocumentStore loaded;
    EXPECT_TRUE(loaded.load(path));
    EXPECT_EQ(loaded.size(), 0u);
}

// ===========================================================================
// Phase 9: Update and Remove Tests
// ===========================================================================

// ---------------------------------------------------------------------------
// 43. Update existing document
// ---------------------------------------------------------------------------

TEST(DocumentStoreUpdate, UpdateExistingDocument)
{
    DocumentStore store;
    store.add({1, "original content"});

    EXPECT_TRUE(store.update({1, "updated content"}));

    const auto doc = store.get(1);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "updated content");
}

// ---------------------------------------------------------------------------
// 44. Update missing document returns false
// ---------------------------------------------------------------------------

TEST(DocumentStoreUpdate, UpdateMissingDocumentReturnsFalse)
{
    DocumentStore store;
    store.add({1, "content"});

    EXPECT_FALSE(store.update({2, "new content"}));
    EXPECT_FALSE(store.update({999, "nonexistent"}));
}

// ---------------------------------------------------------------------------
// 45. Update preserves document count
// ---------------------------------------------------------------------------

TEST(DocumentStoreUpdate, UpdatePreservesDocumentCount)
{
    DocumentStore store;
    store.add({1, "alpha"});
    store.add({2, "beta"});
    const std::size_t count = store.size();

    store.update({1, "updated alpha"});

    EXPECT_EQ(store.size(), count);
}

// ---------------------------------------------------------------------------
// 46. Update does not affect other documents
// ---------------------------------------------------------------------------

TEST(DocumentStoreUpdate, UpdateDoesNotAffectOtherDocuments)
{
    DocumentStore store;
    store.add({1, "alpha"});
    store.add({2, "beta"});

    store.update({1, "updated alpha"});

    const auto d2 = store.get(2);
    ASSERT_TRUE(d2.has_value());
    EXPECT_EQ(d2->content, "beta");
}

// ---------------------------------------------------------------------------
// 47. Remove existing document
// ---------------------------------------------------------------------------

TEST(DocumentStoreRemove, RemoveExistingDocument)
{
    DocumentStore store;
    store.add({1, "content"});

    EXPECT_TRUE(store.remove(1));
    EXPECT_FALSE(store.contains(1));
    EXPECT_FALSE(store.get(1).has_value());
}

// ---------------------------------------------------------------------------
// 48. Remove missing document returns false
// ---------------------------------------------------------------------------

TEST(DocumentStoreRemove, RemoveMissingDocumentReturnsFalse)
{
    DocumentStore store;
    store.add({1, "content"});

    EXPECT_FALSE(store.remove(2));
    EXPECT_FALSE(store.remove(999));
}

// ---------------------------------------------------------------------------
// 49. Remove decreases document count
// ---------------------------------------------------------------------------

TEST(DocumentStoreRemove, RemoveDecreasesDocumentCount)
{
    DocumentStore store;
    store.add({1, "alpha"});
    store.add({2, "beta"});
    store.add({3, "gamma"});

    EXPECT_EQ(store.size(), 3u);
    store.remove(2);
    EXPECT_EQ(store.size(), 2u);
}

// ---------------------------------------------------------------------------
// 50. Remove does not affect other documents
// ---------------------------------------------------------------------------

TEST(DocumentStoreRemove, RemoveDoesNotAffectOtherDocuments)
{
    DocumentStore store;
    store.add({1, "alpha"});
    store.add({2, "beta"});
    store.add({3, "gamma"});

    store.remove(2);

    EXPECT_TRUE(store.contains(1));
    EXPECT_FALSE(store.contains(2));
    EXPECT_TRUE(store.contains(3));

    const auto d1 = store.get(1);
    ASSERT_TRUE(d1.has_value());
    EXPECT_EQ(d1->content, "alpha");

    const auto d3 = store.get(3);
    ASSERT_TRUE(d3.has_value());
    EXPECT_EQ(d3->content, "gamma");
}

// ---------------------------------------------------------------------------
// 51. Remove all documents leaves empty store
// ---------------------------------------------------------------------------

TEST(DocumentStoreRemove, RemoveAllLeavesEmptyStore)
{
    DocumentStore store;
    store.add({1, "alpha"});
    store.add({2, "beta"});

    store.remove(1);
    store.remove(2);

    EXPECT_EQ(store.size(), 0u);
    EXPECT_FALSE(store.contains(1));
    EXPECT_FALSE(store.contains(2));
}

// ---------------------------------------------------------------------------
// 52. Double remove is safe
// ---------------------------------------------------------------------------

TEST(DocumentStoreRemove, DoubleRemoveIsSafe)
{
    DocumentStore store;
    store.add({1, "content"});

    EXPECT_TRUE(store.remove(1));
    EXPECT_FALSE(store.remove(1));
    EXPECT_EQ(store.size(), 0u);
}

// ---------------------------------------------------------------------------
// 53. Update then save/load round trip
// ---------------------------------------------------------------------------

TEST(DocumentStoreUpdate, UpdateSurvivesPersistence)
{
    const auto path = temp_path("update_persist");
    TempFile guard{path};

    DocumentStore store;
    store.add({1, "original"});
    store.save(path);

    store.update({1, "updated"});
    store.save(path);

    DocumentStore loaded;
    EXPECT_TRUE(loaded.load(path));
    EXPECT_EQ(loaded.size(), 1u);
    EXPECT_EQ(loaded.get(1)->content, "updated");
}

// ---------------------------------------------------------------------------
// 54. Remove then save/load round trip
// ---------------------------------------------------------------------------

TEST(DocumentStoreRemove, RemoveSurvivesPersistence)
{
    const auto path = temp_path("remove_persist");
    TempFile guard{path};

    DocumentStore store;
    store.add({1, "alpha"});
    store.add({2, "beta"});
    store.save(path);

    store.remove(1);
    store.save(path);

    DocumentStore loaded;
    EXPECT_TRUE(loaded.load(path));
    EXPECT_EQ(loaded.size(), 1u);
    EXPECT_FALSE(loaded.contains(1));
    EXPECT_TRUE(loaded.contains(2));
}

// ---------------------------------------------------------------------------
// 54. Update doc_id 0
// ---------------------------------------------------------------------------

TEST(DocumentStoreUpdate, UpdateDocIdZero)
{
    DocumentStore store;
    store.add({0, "zero original"});

    EXPECT_TRUE(store.update({0, "zero updated"}));

    const auto doc = store.get(0);
    ASSERT_TRUE(doc.has_value());
    EXPECT_EQ(doc->content, "zero updated");
}

// ---------------------------------------------------------------------------
// 55. Remove doc_id 0
// ---------------------------------------------------------------------------

TEST(DocumentStoreRemove, RemoveDocIdZero)
{
    DocumentStore store;
    store.add({0, "zero"});
    EXPECT_TRUE(store.remove(0));
    EXPECT_FALSE(store.contains(0));
    EXPECT_EQ(store.size(), 0u);
}
