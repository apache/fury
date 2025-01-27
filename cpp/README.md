# Apache Fury™ C++

Fury is a blazingly-fast multi-language serialization framework powered by just-in-time compilation and zero-copy.

## Build Fury C++

```bash
# Build all projects
bazel build //:all
# Run all tests
bazel test //:all
```

## Environment

- Bazel version: 6.3.2

## Benchmark

```bash
bazel build //cpp/fury/benchmark:all
bazel test //cpp/fury/benchmark:all
# You can also run a single benchmark to see how efficient it is.
# For example
bazel run //cpp/fury/benchmark:benchmark_string_util
```
