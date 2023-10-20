# How to build to Fury

## Get the source code

Github repo: https://github.com/alipay/fury

## Building Fury 🏋🏿‍♀️

### Building Fury Java

```bash
cd java
mvn clean compile -DskipTests
```

### Building Fury Python

```bash
cd python
pip install pyarrow==6.0.1 Cython wheel numpy pytest
pip install -v -e .
```

### Building Fury C++

Build fury_util.so:

```bash
bazel build //src/fury/util:fury_util
```

Build fury row format：

```bash
pip install pyarrow==6.0.1
bazel build //src/fury/row:fury_row_format
```

### Building Fury GoLang

```bash
cd go/fury
# run test
go test -v
# run xlang test
go test -v fury_xlang_test.go
```

