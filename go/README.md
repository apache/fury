# Apache Fory™ Go

Fory is a blazingly fast multi-language serialization framework powered by just-in-time compilation and zero-copy.

Currently, Fory Go is implemented using reflection. In the future, we plan to implement a static code generator
to generate serializer code ahead to speed up serialization, or implement a JIT framework which generate ASM
instructions to speed up serialization.

## How to test

```bash
cd go/fory
go test -v
go test -v fory_xlang_test.go
```

## Code Style

```bash
cd go/fory
gofmt -s -w .
```

When using Go's gofmt -s -w . command on Windows, ensure your source files use Unix-style line endings (LF) instead of Windows-style (CRLF). Go tools expect LF by default, and mismatched line endings may cause unexpected behavior or unnecessary changes in version control.

Before committing, you can use `git config core.autocrlf input` to take effect on future commits.
