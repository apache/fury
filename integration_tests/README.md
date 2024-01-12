Integration tests for fury:
- [jdk_compatibility_tests](jdk_compatibility_tests): test fury compatibility across multiple jdk versions.
- [latest_jdk_tests](latest_jdk_tests): test latest jdk.
- [graalvm_tests](graalvm_tests): test graalvm native image support.

> Note that this integration_tests is not designed as a maven multi-module project on purpose, so we can introduce features of higher jdk version without breaking compilation for lower jdk, and add integration tests for other languages.
