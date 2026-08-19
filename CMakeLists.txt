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
# Third-party dependencies (header-only, fetched via FetchContent).
# These are needed by dse_core (HTTP server + JSON serialization) and must
# be available before the library target is defined.
# ---------------------------------------------------------------------------
include(FetchContent)

# Disable auto-detection of optional dependencies that conflict with MinGW/MSYS2.
set(HTTPLIB_USE_OPENSSL_IF_AVAILABLE OFF CACHE BOOL "" FORCE)
set(HTTPLIB_USE_ZLIB_IF_AVAILABLE OFF CACHE BOOL "" FORCE)
set(HTTPLIB_USE_BROTLI_IF_AVAILABLE OFF CACHE BOOL "" FORCE)
set(HTTPLIB_USE_ZSTD_IF_AVAILABLE OFF CACHE BOOL "" FORCE)
set(HTTPLIB_USE_NON_BLOCKING_GETADDRINFO OFF CACHE BOOL "" FORCE)
set(HTTPLIB_INSTALL OFF CACHE BOOL "" FORCE)
set(HTTPLIB_COMPILE OFF CACHE BOOL "" FORCE)

FetchContent_Declare(
    cpp-httplib
    GIT_REPOSITORY https://github.com/yhirose/cpp-httplib.git
    GIT_TAG        v0.23.0
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(cpp-httplib)

FetchContent_Declare(
    nlohmann_json
    GIT_REPOSITORY https://github.com/nlohmann/json.git
    GIT_TAG        v3.12.0
    GIT_SHALLOW    TRUE
)
FetchContent_MakeAvailable(nlohmann_json)

# ---------------------------------------------------------------------------
# Core library. Shared by the application and the tests.
# The include directory is PUBLIC so consumers can #include "tokenizer.h"
# cleanly; warnings are PRIVATE so they do not leak into consumers.
# ---------------------------------------------------------------------------
add_library(dse_core STATIC
    src/tokenizer.cpp
    src/inverted_index.cpp
    src/query_processor.cpp
    src/ranker.cpp
    src/search_service.cpp
    src/http_server.cpp)
target_include_directories(dse_core PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_compile_options(dse_core PRIVATE ${PROJECT_WARNINGS})
target_link_libraries(dse_core PUBLIC httplib::httplib nlohmann_json::nlohmann_json)

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

    add_executable(query_processor_test tests/query_processor_test.cpp)
    target_compile_options(query_processor_test PRIVATE ${PROJECT_WARNINGS})
    target_link_libraries(query_processor_test PRIVATE GTest::gtest_main dse_core)

    add_executable(ranker_test tests/ranker_test.cpp)
    target_compile_options(ranker_test PRIVATE ${PROJECT_WARNINGS})
    target_link_libraries(ranker_test PRIVATE GTest::gtest_main dse_core)

    add_executable(search_service_test tests/search_service_test.cpp)
    target_compile_options(search_service_test PRIVATE ${PROJECT_WARNINGS})
    target_link_libraries(search_service_test PRIVATE GTest::gtest_main dse_core)

    add_executable(http_api_test tests/http_api_test.cpp)
    target_compile_options(http_api_test PRIVATE ${PROJECT_WARNINGS})
    target_link_libraries(http_api_test PRIVATE GTest::gtest_main dse_core)

    add_executable(app_integration_test tests/app_integration_test.cpp)
    target_compile_options(app_integration_test PRIVATE ${PROJECT_WARNINGS})
    target_link_libraries(app_integration_test PRIVATE GTest::gtest_main dse_core)

    include(GoogleTest)
    gtest_discover_tests(smoke_test)
    gtest_discover_tests(tokenizer_test)
    gtest_discover_tests(inverted_index_test)
    gtest_discover_tests(query_processor_test)
    gtest_discover_tests(ranker_test)
    gtest_discover_tests(search_service_test)
    gtest_discover_tests(http_api_test)
    gtest_discover_tests(app_integration_test)
endif()
