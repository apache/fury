# Apache Fury™ C++

## Build Fury Python

```bash
bazel build //:all
bazel test //:all
```

## Environment

- Bazel version: 6.3.2

## BenchMark

```bash
bazel build //cpp/fury/benchmark:all
bazel run //cpp/fury/benchmark:(target)
bazel test //cpp/fury/benchmark:all
```
