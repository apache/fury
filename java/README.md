# Apache Fory™ Java

## Code format

```bash
mvn -T10 spotless:apply
mvn -T10 checkstyle:check
```

## Testing

```bash
mvn -T10 test -Dcheckstyle.skip -Dmaven.javadoc.skip
```
