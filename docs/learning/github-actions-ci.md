# GitHub Actions CI

## What Is CI?

**Continuous Integration (CI)** is a development practice where code changes are automatically built and tested whenever they are pushed to a repository or submitted as a pull request. This catches build failures and test regressions early, before they reach the main branch.

## What GitHub Actions Does in This Repository

The CI workflow (`.github/workflows/ci.yml`) runs automatically on:

- **Every push** to any branch
- **Every pull request** targeting any branch

When triggered, it:

1. Checks out the repository code
2. Installs build dependencies (CMake, GCC, Ninja)
3. Configures the project with CMake
4. Builds the complete project
5. Runs the complete test suite via CTest

If any step fails — CMake configuration error, compilation failure, or test failure — the workflow reports a failure on GitHub.

## How the Build Works

```
cmake -B build-ci -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-ci -j$(nproc)
ctest --test-dir build-ci --output-on-failure
```

- **CMake** configures the project and fetches dependencies (GoogleTest, cpp-httplib, nlohmann/json) via FetchContent
- **Ninja** builds all targets in parallel
- **CTest** discovers and runs all GoogleTest test cases registered via `gtest_discover_tests()`

## How Failed Tests Appear on GitHub

When you push code or open a pull request:

1. Go to the repository on GitHub
2. Click the **Actions** tab
3. Click the workflow run
4. If a test fails, the step will show a red ❌ with `--output-on-failure` showing which test(s) failed and their error output

## Local Reproduction

To reproduce the CI build locally on Linux:

```bash
cmake -B build-ci -G Ninja -DCMAKE_BUILD_TYPE=Release
cmake --build build-ci -j$(nproc)
ctest --test-dir build-ci --output-on-failure
```

On Windows (MSYS2/MinGW):

```bash
cmake -B build-ci -G Ninja
cmake --build build-ci -j
ctest --test-dir build-ci --output-on-failure
```

## Notes

- The workflow uses `ubuntu-latest` (Ubuntu 22.04+)
- GCC on ubuntu-latest supports C++20, which this project requires
- Dependencies are fetched at configure time (no vendored dependencies)
- The workflow does not deploy or publish anything — it only builds and tests
