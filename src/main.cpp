// Distributed Search Engine - Phase 0 (Project Foundation).
//
// This executable exists to prove that the project builds and runs under
// C++20. No search-engine functionality is implemented yet.

#include <iostream>
#include <string_view>

namespace {

constexpr std::string_view kAppName = "DistributedSearchEngine";
constexpr std::string_view kPhase   = "Phase 0 - Project Foundation";

} // namespace

int main()
{
    std::cout << kAppName << " | " << kPhase << '\n';
    std::cout << "Built with C++ standard: " << __cplusplus << '\n';
    return 0;
}
