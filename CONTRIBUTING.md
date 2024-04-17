# How to contribute to Fury

## Finding good first issues

See [Good First Issues](https://github.com/apache/incubator-fury/contribute).

## How to create an issue

Create an issue with [this form](https://github.com/apache/incubator-fury/issues/new/choose).

## How to title your PR

Generally we follows the [Conventional Commits](https://www.conventionalcommits.org/) for pull request titles, 
since we will squash and merge the PR and use the PR title as the first line of commit message.

For example, here are good PR titles:
- feat(java): support xxx feature
- fix(c++): blablabla
- chore(python): remove useless yyy file

If the submitted PR affects the performance of Fury, we strongly recommend using the perf type,
and need to provide benchmark data in the PR description. For how to run the benchmark,
please check [Fury Java Benchmark](https://github.com/apache/incubator-fury/blob/main/java/benchmark/README.md).

For more details, please check [pr-lint.yml](./.github/workflows/pr-lint.yml).

## Testing

For environmental requirements, please check [DEVELOPMENT.md](./docs/guide/DEVELOPMENT.md).

### Python

```bash
cd python
pytest -v -s .
```

### Java

```bash
cd java
mvn -T10 clean test
```

### C++

```bash
bazel test $(bazel query //...)
```

### GoLang

```bash
cd go/fury
go test -v
go test -v fury_xlang_test.go
```

### Rust

```bash
cd rust
cargo test
```

### JavaScript

```bash
cd javascript
npm run test
```

## Code Style

Run all checks: `bash ci/format.sh --all`.

### License headers

```bash
docker run --rm -v $(pwd):/github/workspace ghcr.io/korandoru/hawkeye-native:v3 format
```

### Java

```bash
cd java
# code format
mvn spotless:apply
# code format check
mvn spotless:check
mvn checkstyle:check
```

### Python

```bash
cd python
# install dependencies fro styling
pip install black==22.1.0 flake8==3.9.1 flake8-quotes flake8-bugbear
# format python code
black pyfury
```

### C++

```bash
git ls-files -- '*.cc' '*.h' | xargs -P 5 clang-format -i
```

### GoLang

```bash
cd go/fury
gofmt -s -w .
```

### Rust

```bash
cd rust
cargo fmt
```

### JavaScript

```bash
cd javascript
npm run lint
```

## Debug

### Java

Fury supports dump jit generated code into local file for better debug by configuring environment variables:

- `FURY_CODE_DIR`：The directory for fury to dump generated code. Set to empty by default to skip dump code.
- `ENABLE_FURY_GENERATED_CLASS_UNIQUE_ID`: Append an unique id for dynamically generated files by default to avoid serializer collision for different classes with same name. Set this to `false` to keep serializer name same for multiple execution or `AOT` codegen.

By using those environment variables, we can generate code to source directory and debug the generated code in next run.

### Python

```bash
cd python
python setup.py develop
```

* Use `cython --cplus -a  pyfury/_serialization.pyx` to produce an annotated HTML file of the source code. Then you can analyze interaction between Python objects and Python's C API.
* Read more: https://cython.readthedocs.io/en/latest/src/userguide/debugging.html

```bash
FURY_DEBUG=true python setup.py build_ext --inplace
# For linux
cygdb build
```

### C++

See the [Debugging C++](docs/cpp_debug.md) doc.

### Debug Crash

Enable core dump on Macos Monterey 12.1:

```bash
/usr/libexec/PlistBuddy -c "Add :com.apple.security.get-task-allow bool true" tmp.entitlements
codesign -s - -f --entitlements tmp.entitlements /Users/chaokunyang/anaconda3/envs/py3.8/bin/python
ulimit -c unlimited
```

then run the code:

```bash
python fury_serializer.py
ls -al /cores
```

## Profiling

### C++

```bash
# Dtrace
sudo dtrace -x ustackframes=100 -n 'profile-99 /pid == 73485 && arg1/ { @[ustack()] = count(); } tick-60s { exit(0); }' -o out.stack
sudo stackcollapse.pl out.stack > out.folded
sudo flamegraph.pl out.folded > out.svg
```

## Website

Fury's website consists of static pages hosted at https://github.com/apache/incubator-fury-site.

All updates about docs under [guide](docs/guide) and [benchmarks](docs/benchmarks) will be synced to the site repo automatically.

If you want write a blog, or update other contents about the website, please submit PR to the site repo.
