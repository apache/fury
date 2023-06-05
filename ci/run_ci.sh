#! /usr/bin/env bash
set -e
set -x

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

install_bazel() {
  URL="https://github.com/bazelbuild/bazel/releases/download/4.2.3/bazel-4.2.3-installer-linux-x86_64.sh"
  wget -q -O install.sh $URL
  chmod +x install.sh
  ./install.sh --user
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
         exit $testcode
      fi
      echo "Executing fury javascript tests succeeds"
    ;;
    cpp)
      echo "Install pyarrow"
      pip install pyarrow==4.0.0
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