# Fury Java 
## Code format
```bash
mvn -T10 license:format
mvn -T10 spotless:apply
mvn -T10 checkstyle:check
```

## Testing
```bash
mvn -T10 test -Dcheckstyle.skip -Dlicense.skip -Dmaven.javadoc.skip
```