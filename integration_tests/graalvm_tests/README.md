# Graalvm native image Tests

Examples and tests for Fory serialization in graalvm native image

## Test

```bash
mvn -DskipTests=true -Pnative package
```

## Benchmark

```bash
BENCHMARK_REPEAT=400000 mvn -Pnative -Dagent=true -DskipTests -DskipNativeBuild=true package exec:exec@java-agent
BENCHMARK_REPEAT=400000 mvn -DskipTests=true -Pnative -Dagent=true package
```

`-Dagent=true` is needed by JDK serialization only to build reflection config, it's not needed for fory serialization.
