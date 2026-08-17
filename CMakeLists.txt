cmake_minimum_required(VERSION 3.24)

project(DistributedSearchEngine
    VERSION 0.1.0
    DESCRIPTION "Distributed search engine built from first principles in C++20"
    LANGUAGES CXX)

# ---------------------------------------------------------------------------
# C++20, required, without compiler extensions.
# ---------------------------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# ---------------------------------------------------------------------------
# Compiler warnings for first-party code only (not for fetched dependencies).
# ---------------------------------------------------------------------------
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    set(PROJECT_WARNINGS -Wall -Wextra -Wpedantic)
elseif(MSVC)
    set(PROJECT_WARNINGS /W4)
endif()

# ---------------------------------------------------------------------------
# Core library. Shared by the application and the tests (Phase 1B).
# The include directory is PUBLIC so consumers can #include "tokenizer.h"
# cleanly; warnings are PRIVATE so they do not leak into consumers.
# ---------------------------------------------------------------------------
add_library(dse_core STATIC
    src/tokenizer.cpp
    src/inverted_index.cpp)
target_include_directories(dse_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_options(dse_core PRIVATE ${PROJECT_WARNINGS})

# ---------------------------------------------------------------------------
# Main executable.
# ---------------------------------------------------------------------------
add_executable(DistributedSearchEngine src/main.cpp)
target_compile_options(DistributedSearchEngine PRIVATE ${PROJECT_WARNINGS})
target_link_libraries(DistributedSearchEngine PRIVATE dse_core)

# ---------------------------------------------------------------------------
# Tests. GoogleTest is fetched at configure time (pinned to a stable release),
# never vendored into this repository.
# ---------------------------------------------------------------------------
include(CTest) # provides the BUILD_TESTING option (ON by default)
if(BUILD_TESTING)
    include(FetchContent)

    FetchContent_Declare(
        googletest
        GIT_REPOSITORY https://github.com/google/googletest.git
        GIT_TAG        v1.18.0
        GIT_SHALLOW    TRUE
    )

    # Minimal, reproducible GoogleTest build: no mock library, no installation.
    set(BUILD_GMOCK   OFF CACHE BOOL "" FORCE)
    set(INSTALL_GTEST OFF CACHE BOOL "" FORCE)

    FetchContent_MakeAvailable(googletest)

    add_executable(smoke_test tests/smoke_test.cpp)
    target_compile_options(smoke_test PRIVATE ${PROJECT_WARNINGS})
    target_link_libraries(smoke_test PRIVATE GTest::gtest_main)

    add_executable(tokenizer_test tests/tokenizer_test.cpp)
    target_compile_options(tokenizer_test PRIVATE ${PROJECT_WARNINGS})
    target_link_libraries(tokenizer_test PRIVATE GTest::gtest_main dse_core)

    add_executable(inverted_index_test tests/inverted_index_test.cpp)
    target_compile_options(inverted_index_test PRIVATE ${PROJECT_WARNINGS})
    target_link_libraries(inverted_index_test PRIVATE GTest::gtest_main dse_core)

    include(GoogleTest)
    gtest_discover_tests(smoke_test)
    gtest_discover_tests(tokenizer_test)
    gtest_discover_tests(inverted_index_test)
endif()
