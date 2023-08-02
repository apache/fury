#!/usr/bin/env bash

# Copyright 2023 The Fury Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e
set -x

ROOT="$(git rev-parse --show-toplevel)"
echo "Root path: $ROOT, home path: $HOME"
cd "$ROOT"

install_python() {
  wget -q https://repo.anaconda.com/miniconda/Miniconda3-py38_23.5.2-0-Linux-x86_64.sh -O Miniconda3.sh
  bash Miniconda3.sh -b -p $HOME/miniconda && rm -f miniconda.*
  which python
  echo "Python version $(python -V)"
}

install_pyfury() {
  export PATH="$HOME/miniconda/bin:$PATH"
  echo "Python version $(python -V)"
  pip install pyarrow==4.0.0 Cython wheel numpy pytest
  pushd "$ROOT/python"
  pip list
  export PATH=~/bin:$PATH
  echo "Install pyfury"
  pip install -v -e .
  popd
}

install_bazel() {
  if command -v java >/dev/null; then
    echo "existing bazel location $(which bazel)"
    echo "existing bazel version $(bazel version)"
  fi
  URL="https://github.com/bazelbuild/bazel/releases/download/4.2.0/bazel-4.2.0-installer-linux-x86_64.sh"
  wget -q -O install.sh $URL
  chmod +x install.sh
  set +x
  ./install.sh --user
  source ~/.bazel/bin/bazel-complete.bash
  set -x
  export PATH=~/bin:$PATH
  echo "$HOME/bin/bazel version: $(~/bin/bazel version)"
  rm -f install.sh
  VERSION=`bazel version`
  echo "bazel version: $VERSION"
  MEM=`cat /proc/meminfo | grep MemTotal | awk '{print $2}'`
  JOBS=`expr $MEM / 1024 / 1024 / 3`
  echo "build --jobs="$JOBS >> ~/.bazelrc
  grep "jobs" ~/.bazelrc
}

case $1 in
    javascript)
      set +e
      echo "Executing fury javascript tests"
      cd "$ROOT/javascript"
      npm install
      node ./node_modules/.bin/jest --ci --reporters=default --reporters=jest-junit
      testcode=$?
      if [[ $testcode -ne 0 ]]; then
        echo "Executing fury javascript tests failed"
        # TODO(bigtech) enable js ci
        # exit $testcode
      fi
      echo "Executing fury javascript tests succeeds"
    ;;
    java8)
      echo "Executing fury java tests"
      cd "$ROOT/java"
      set +e
      mvn -T16 --batch-mode test
      testcode=$?
      if [[ $testcode -ne 0 ]]; then
        exit $testcode
      fi
      echo "Executing fury java tests succeeds"
    ;;
   java11)
      java -version
      echo "Executing fury java tests"
      cd "$ROOT/java"
      set +e
      mvn -T16 --batch-mode test
      testcode=$?
      if [[ $testcode -ne 0 ]]; then
        exit $testcode
      fi
      echo "Executing fury java tests succeeds"
    ;;
   java*)
      java -version
      echo "Executing fury java tests"
      cd "$ROOT/java"
      set +e
      mvn -T16 --batch-mode test -pl '!fury-format,!fury-testsuite,!fury-benchmark'
      testcode=$?
      if [[ $testcode -ne 0 ]]; then
        exit $testcode
      fi
      echo "Executing fury java tests succeeds"
    ;;
    rust)
      set +e
      echo "Executing fury rust tests"
      cd "$ROOT/rust"
      cargo test
      rustup component add clippy-preview
      rustup component add rustfmt
      cargo clippy -- -Dwarnings
      cargo fmt --check
    ;;
    cpp)
      echo "Install pyarrow"
      pip install pyarrow==4.0.0
      export PATH=~/bin:$PATH
      echo "bazel version: $(bazel version)"
      set +e
      echo "Executing fury c++ tests"
      bazel test $(bazel query //...)
      testcode=$?
      if [[ $testcode -ne 0 ]]; then
        echo "Executing fury c++ tests failed"
        exit $testcode
      fi
      echo "Executing fury c++ tests succeeds"
    ;;
    python)
      install_pyfury
      pip install pandas
      cd "$ROOT/python"
      echo "Executing fury python tests"
      pytest -v -s --durations=60 pyfury/tests
      testcode=$?
      if [[ $testcode -ne 0 ]]; then
        exit $testcode
      fi
      echo "Executing fury python tests succeeds"
      ;;
    format)
      echo "Install format tools"
      pip install black==22.1.0 flake8==3.9.1 flake8-quotes flake8-bugbear click==8.0.2
      echo "Executing format check"
      bash ci/format.sh
      cd "$ROOT/java"
      mvn -T10 -B spotless:check
      mvn -T10 -B checkstyle:check
      echo "Executing format check succeeds"
    ;;
    *)
      echo "Execute command $*"
      "$@"
      ;;
esac