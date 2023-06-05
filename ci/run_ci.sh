#! /usr/bin/env bash
set -e
set -x

ROOT="$(git rev-parse --show-toplevel)"
echo "Root path: $ROOT, home path: $HOME"
cd "$ROOT"

install_bazel() {
  if command -v java >/dev/null; then
    echo "existing bazel location $(which bazel)"
    echo "existing bazel version $(bazel version)"
  fi
  URL="https://github.com/bazelbuild/bazel/releases/download/3.4.0/bazel-3.4.0-installer-linux-x86_64.sh"
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
    java8)
      echo "Executing fury java tests"
      cd "$ROOT/java"
      # check google java style
      mvn -T16 -B spotless:check
      # check naming and others
      mvn -T16 checkstyle:check
      set +e
      mvn -T16 test
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
      # check google java style
      mvn -T16 -B spotless:check
      # check naming and others
      mvn -T16 checkstyle:check
      set +e
      mvn -T16 test
      testcode=$?
      if [[ $testcode -ne 0 ]]; then
        exit $testcode
      fi
      echo "Executing fury java tests succeeds"
    ;;
   java17)
      java -version
      echo "Executing fury java tests"
      cd "$ROOT/java"
      # check google java style
      mvn -T16 -B spotless:check
      # check naming and others
      mvn -T16 checkstyle:check
      set +e
      mvn -T16 test -pl '!fury-format,!fury-testsuite,!fury-benchmark'
      testcode=$?
      if [[ $testcode -ne 0 ]]; then
        exit $testcode
      fi
      echo "Executing fury java tests succeeds"
    ;;
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
    format)
      echo "Executing format check"
      cd "$ROOT/java"
      mvn -T10 license:format
      mvn -T10 spotless:apply
      mvn -T10 checkstyle:check
    ;;
    *)
      echo "Execute command $*"
      "$@"
      ;;
esac