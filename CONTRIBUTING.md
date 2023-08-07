# How to contribute to Fury
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
pip install pyarrow==7.0.0 Cython wheel numpy pytest
pip install -v -e .
```

### Building Fury C++
Build fury_util.so:
```bash
bazel build //src/fury/util:fury_util
```
Build fury row format：
```bash
pip install pyarrow==7.0.0
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

## Finding good first issues 🔎
See [Good First Issue](https://github.com/alipay/fury/issues?q=is%3Aopen+is%3Aissue+label%3A%22good+first+issue%22) for open good first issues.

## How to create an issue
See https://github.com/alipay/fury/issues for open issues.

## Testing 🧪
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
Run c++ tests：
```bash
bazel test $(bazel query //...)
```

### GoLang
```bash
cd go/fury
# run tests
go test -v
# run xlang tests
go test -v fury_xlang_test.go
```

## Styling 😎

Run all checks: `bash ci/format.sh --all`

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

## Debug
### Java Debug
#### JIT DEBUG
Fury supports dump jit generated code into local file for better debug by configuring environment variables:
- `FURY_CODE_DIR`：The directory for fury to dump generated code. Set to empty by default to skip dump code.
- `ENABLE_FURY_GENERATED_CLASS_UNIQUE_ID`: Append an unique id for dynamically generated files by default to avoid serializer collision for different classes with same name. Set this to `false` to keep serializer name same for multiple execution or `AOT` codegen. 

By using those environment variables, we can generate code to source directory and debug the generated code in next run.

### Python Debug
```bash
cd python
python setup.py develop
```
* cython: use `cython --cplus -a  pyfury/_serialization.pyx` to produce an annotated
  html file of the source code. Then we can analyze interaction between
  Python objects and Python’s C-API.

* Debug
  https://cython.readthedocs.io/en/latest/src/userguide/debugging.html
```bash
FURY_DEBUG=true python setup.py build_ext --inplace
# For linux
cygdb build
```

### C++ debug
See [cpp_debug](https://github.com/alipay/fury/blob/main/docs/cpp_debug.md) doc.

### Debug Crash
Enable core dump on Macos Monterey 12.1
```bash
 /usr/libexec/PlistBuddy -c "Add :com.apple.security.get-task-allow bool true" tmp.entitlements     
codesign -s - -f --entitlements tmp.entitlements /Users/chaokunyang/anaconda3/envs/py3.8/bin/python
ulimit -c unlimited

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

## CI
### Login into ci machine
```yaml
      - name: Setup tmate session
        uses: mxschmitt/action-tmate@v3
```

## Website
Fury website are static pages hosted by github pages under https://github.com/fury-project/fury-sites.

All updates about docs under [guide](./docs/guide) and [benchmarks](./docs/benchmarks) will be synced to [fury-sites](https://github.com/fury-project/fury-sites) automatically.

If you want write a blog, or update other contents about fury website, please submit PR to [fury-sites](https://github.com/fury-project/fury-sites).