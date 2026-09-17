// Distributed Search Engine - Tokenizer tests (Phase 1B-2).
//
// Real coverage for dse::tokenize per the Phase 1A testing strategy
// (docs/learning/phase-1-tokenizer.md, Section 14). The tests compare the
// COMPLETE returned vector: contents, order, and duplicates.
//
// Groups: basic tokenization, normalization, punctuation, whitespace,
// hyphen handling, duplicates, numbers/symbols, edge cases.

#include <gtest/gtest.h>

#include "tokenizer.h"

#include <string>
#include <string_view>
#include <vector>

namespace {

// Single-point comparison helper: asserts the full tokenize result equals
// the expected vector (contents + order + duplicates).
void expect_tokens(std::string_view input, std::vector<std::string> expected)
{
    EXPECT_EQ(dse::tokenize(input), expected);
}

} // namespace

// ---------------------------------------------------------------------------
// 1. Basic tokenization
// ---------------------------------------------------------------------------

TEST(TokenizeBasic, SingleWord)
{
    expect_tokens("hello", {"hello"});
}

TEST(TokenizeBasic, SplitsSpaceSeparatedWords)
{
    expect_tokens("hello world", {"hello", "world"});
}

TEST(TokenizeBasic, MultipleWordsPreserveOrder)
{
    expect_tokens("one two three", {"one", "two", "three"});
}

// ---------------------------------------------------------------------------
// 2. Normalization
// ---------------------------------------------------------------------------

TEST(TokenizeNormalization, LowercasesAllUppercase)
{
    expect_tokens("HELLO", {"hello"});
}

TEST(TokenizeNormalization, MixedCaseWord)
{
    expect_tokens("HeLLo", {"hello"});
}

TEST(TokenizeNormalization, MixedCaseAcrossWords)
{
    expect_tokens("Search SEARCH search", {"search", "search", "search"});
}

// ---------------------------------------------------------------------------
// 3. Punctuation
// ---------------------------------------------------------------------------

TEST(TokenizePunctuation, RemovesSurroundingPunctuation)
{
    expect_tokens("hello, world!", {"hello", "world"});
}

TEST(TokenizePunctuation, PunctuationBetweenWords)
{
    expect_tokens("word,word", {"word", "word"});
}

TEST(TokenizePunctuation, ConsecutivePunctuation)
{
    expect_tokens("a!!!b", {"a", "b"});
}

TEST(TokenizePunctuation, PunctuationOnlyInput)
{
    expect_tokens("!!!", {});
    expect_tokens("...", {});
}

// ---------------------------------------------------------------------------
// 4. Whitespace
// ---------------------------------------------------------------------------

TEST(TokenizeWhitespace, MultipleSpaces)
{
    expect_tokens("hello     world", {"hello", "world"});
}

TEST(TokenizeWhitespace, LeadingAndTrailingSpaces)
{
    expect_tokens("   hello world   ", {"hello", "world"});
}

TEST(TokenizeWhitespace, TabsAreSeparators)
{
    expect_tokens("\thello\tworld", {"hello", "world"});
}

TEST(TokenizeWhitespace, NewlinesAreSeparators)
{
    expect_tokens("hello\nworld", {"hello", "world"});
}

TEST(TokenizeWhitespace, MixedWhitespace)
{
    expect_tokens(" \t\n hello \r\n world \n", {"hello", "world"});
}

// ---------------------------------------------------------------------------
// 5. Hyphen handling
// ---------------------------------------------------------------------------

TEST(TokenizeHyphen, SplitsHyphenatedWord)
{
    expect_tokens("search-engine", {"search", "engine"});
}

TEST(TokenizeHyphen, MultipleHyphens)
{
    expect_tokens("a--b", {"a", "b"});
}

TEST(TokenizeHyphen, HyphenOnlyInput)
{
    expect_tokens("---", {});
}

// ---------------------------------------------------------------------------
// 6. Duplicates
// ---------------------------------------------------------------------------

TEST(TokenizeDuplicates, PreservesRepeatedWords)
{
    expect_tokens("cat cat dog", {"cat", "cat", "dog"});
}

TEST(TokenizeDuplicates, PreservesManyDuplicates)
{
    expect_tokens("the the the", {"the", "the", "the"});
}

TEST(TokenizeDuplicates, DuplicatePositionsAndOrder)
{
    expect_tokens("a b a b", {"a", "b", "a", "b"});
}

// ---------------------------------------------------------------------------
// 7. Numbers and symbols
// ---------------------------------------------------------------------------

TEST(TokenizeNumbersSymbols, Integer)
{
    expect_tokens("42", {"42"});
}

TEST(TokenizeNumbersSymbols, LeadingZerosPreserved)
{
    expect_tokens("007", {"007"});
}

TEST(TokenizeNumbersSymbols, DecimalSplitsAtPoint)
{
    expect_tokens("3.14", {"3", "14"});
}

TEST(TokenizeNumbersSymbols, CurrencySplits)
{
    expect_tokens("$9.99", {"9", "99"});
}

TEST(TokenizeNumbersSymbols, ThousandsSeparatorSplits)
{
    expect_tokens("1,000", {"1", "000"});
}

TEST(TokenizeNumbersSymbols, ParenthesizedNumber)
{
    expect_tokens("(2024)", {"2024"});
}

TEST(TokenizeNumbersSymbols, DigitsStayInsideTokens)
{
    expect_tokens("abc123", {"abc123"});
}

TEST(TokenizeNumbersSymbols, SignsAreSeparators)
{
    expect_tokens("+5 -5", {"5", "5"});
}

TEST(TokenizeNumbersSymbols, ApostropheSplits)
{
    expect_tokens("don't", {"don", "t"});
}

TEST(TokenizeNumbersSymbols, UnderscoreSplits)
{
    expect_tokens("under_score", {"under", "score"});
}

TEST(TokenizeNumbersSymbols, SlashSplits)
{
    expect_tokens("and/or", {"and", "or"});
    expect_tokens("1/2", {"1", "2"});
}

TEST(TokenizeNumbersSymbols, PlusSplits)
{
    expect_tokens("C++", {"c"});
    expect_tokens("a+b", {"a", "b"});
}

TEST(TokenizeNumbersSymbols, AmpersandSplits)
{
    expect_tokens("rock&roll", {"rock", "roll"});
}

TEST(TokenizeNumbersSymbols, EmailFragments)
{
    expect_tokens("user@example.com", {"user", "example", "com"});
}

TEST(TokenizeNumbersSymbols, UrlFragments)
{
    expect_tokens("https://example.com/path?q=1",
                  {"https", "example", "com", "path", "q", "1"});
}

// ---------------------------------------------------------------------------
// 8. Edge cases
// ---------------------------------------------------------------------------

TEST(TokenizeEdgeCases, EmptyInput)
{
    expect_tokens("", {});
}

TEST(TokenizeEdgeCases, WhitespaceOnlyInput)
{
    expect_tokens("   ", {});
}

TEST(TokenizeEdgeCases, TabNewlineCarriageReturnOnly)
{
    expect_tokens("\t\n\r", {});
}

TEST(TokenizeEdgeCases, NonAsciiBytesAreSeparators)
{
    expect_tokens("café", {"caf"});
}

TEST(TokenizeEdgeCases, NonAsciiOnlyInput)
{
    expect_tokens("你好", {});
}

TEST(TokenizeEdgeCases, MixedAsciiAndNonAscii)
{
    expect_tokens("naïve naive", {"na", "ve", "naive"});
}

TEST(TokenizeEdgeCases, LongInputPreservesOrderAndDuplicates)
{
    constexpr int kCount = 10'000;
    const std::string word = "search";

    std::string input;
    input.reserve(word.size() * static_cast<std::size_t>(kCount) + kCount - 1);
    for (int i = 0; i < kCount; ++i) {
        if (i > 0) {
            input += ' ';
        }
        input += word;
    }

    const auto tokens = dse::tokenize(input);
    ASSERT_EQ(tokens.size(), static_cast<std::size_t>(kCount));
    for (const auto& token : tokens) {
        EXPECT_EQ(token, word);
    }
}

TEST(TokenizeEdgeCases, DeterministicSameInputSameOutput)
{
    const std::string_view input = "Hello, WORLD! search-engine 42";
    EXPECT_EQ(dse::tokenize(input), dse::tokenize(input));
}

TEST(TokenizeEdgeCases, InputStringViewNeedNotOutliveCall)
{
    // Passing a temporary std::string is safe: tokenize never stores the view.
    expect_tokens(std::string{"temporary input"}, {"temporary", "input"});
}
