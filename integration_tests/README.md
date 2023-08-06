Integration tests for fury:
- [jdk_compatibility_tests](jdk_compatibility_tests): test fury compatibility across multiple jdk versions.
- [perftests](perftests): benchmark with protobuf/flatbuffers directly.

> Note that this integration_tests is not designed as a maven multi-module project on purpose, so we can introduce features of higher jdk version without breaking compilation for lower jdk, and add integration tests for other languages.
