// Phase 0 smoke test.
//
// Purpose: prove that GoogleTest is integrated into the build, that the test
// executable compiles and links, and that CTest can discover and run the
// tests. Real unit tests for search-engine components arrive in Phase 1.

#include <gtest/gtest.h>

#include <string_view>

namespace {

// Trivial pure helper so the test exercises a real function under test.
int add_one(int value) { return value + 1; }

} // namespace

TEST(SmokeTest, AddOneIncrementsValue)
{
    EXPECT_EQ(add_one(41), 42);
}

TEST(SmokeTest, Cpp20FeatureAvailable)
{
    const std::string_view greeting{"smoke"};
    ASSERT_EQ(greeting, "smoke");
    EXPECT_EQ(greeting.size(), 5U);
}
