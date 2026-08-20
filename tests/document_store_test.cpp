// Distributed Search Engine - Document Store tests (Phase 6B-1).
//
// Tests for dse::DocumentStore covering: basic add/get, contains,
// missing documents, size tracking, multiple documents, duplicate
// ID rejection, duplicate does not overwrite, doc_id 0, empty content,
// long content, and deterministic behavior.

#include <gtest/gtest.h>

#include "document_store.h"

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace {

using dse::Document;
using dse::DocumentStore;

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
