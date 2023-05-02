#! /usr/bin/env bash
set -e
set -x

ROOT="$(git rev-parse --show-toplevel)"
cd "$ROOT"

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
    format)
      echo "Executing format check"
      cd "$ROOT/java"
      mvn -T10 license:format
      mvn -T10 spotless:apply
      mvn -T10 checkstyle:check
    ;;
esac