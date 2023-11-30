# Graalvm native image Tests
Examples and tests for Fury serialization in graalvm native image

## Test
```bash
mvn -DskipTests=true -Pnative package
```

## Benchmark
```bash
mvn -Pnative -Dagent=true -DskipTests -DskipNativeBuild=true package exec:exec@java-agent
mvn -DskipTests=true -Pnative -Dagent=true package
```
`-Dagent=true` is optional, just for JDK serialization reflection config collection, if not needed for fury serialization