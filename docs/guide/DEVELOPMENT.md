---
title: Development
sidebar_position: 7
id: development
---

# How to build Fury

Please checkout the source tree from https://github.com/apache/fury.

### Build Fury Java

```bash
cd java
mvn clean compile -DskipTests
```

#### Environment Requirements

- java 1.8+
- maven 3.6.3+

### Build Fury Python

```bash
cd python
pip install pyarrow==14.0.0 Cython wheel numpy pytest
pip install -v -e .
```

#### Environment Requirements

- python 3.6+

### Build Fury C++

Build fury row format：

```bash
pip install pyarrow==14.0.0
bazel build //cpp/fury/row:fury_row_format
```

Build fury row format encoder:

```bash
pip install pyarrow==14.0.0
bazel build //cpp/fury/encoder:fury_encoder
```

#### Environment Requirements

- compilers with C++17 support
- bazel 6.3.2

### Build Fury GoLang

```bash
cd go/fury
# run test
go test -v
# run xlang test
go test -v fury_xlang_test.go
```

#### Environment Requirements

- go 1.13+

### Build Fury Rust

```bash
cd rust
# build
cargo build
# run test
cargo test
```

#### Environment Requirements

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### Build Fury JavaScript

```bash
cd javascript
npm install

# run build
npm run build
# run test
npm run test
```

#### Environment Requirements

- node 14+
- npm 8+

# Update doc

If [FuryBuilder](https://github.com/apache/incubator-fury/blob/main/java/fury-core/src/main/java/org/apache/fury/config/FuryBuilder.java) is modified (including modifying configuration fields or adding new configuration fields), we need to update the table in [java_serialization_guide.md#furybuilder--options](https://github.com/apache/incubator-fury/blob/main/docs/guide/java_serialization_guide.md#furybuilder--options).

We provide a script to automatically update the table. After modifying [FuryBuilder](https://github.com/apache/incubator-fury/blob/main/java/fury-core/src/main/java/org/apache/fury/config/FuryBuilder.java), we can perform the update operation through the following command.

```bash
python3 tools/gen_fury_builder_doc.py
```

To use this script, we need to add comment on the [FuryBuilder](https://github.com/apache/incubator-fury/blob/main/java/fury-core/src/main/java/org/apache/fury/config/FuryBuilder.java) configuration fields in the following format.

```
/**
 * Comment body
 *
 * @defaultValue xxx
 */
```

The `Comment body` corresponds to the `Description` in the table, and the `@defaultValue` corresponds to the `Default Value` in the table.

