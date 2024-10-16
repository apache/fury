# Apache Fury™ Kotlin

This provides additional Fury support for Kotlin Serialization on JVM:

Most standard kotlin types are already supported out of the box with the default Fury java implementation.

Fury Kotlin provides additional tests and implementation support for Kotlin types.

Fury Kotlin is tested and works with the following types:

- primitives: `Byte`, `Boolean`, `Int`, `Short`, `Long`, `Char`, `Float`, `Double`, `UByte`, `UShort`, `UInt`, `UShort`, `UInt`.
- `Byte`, `Boolean`, `Int`, `Short`, `Long`, `Char`, `Float`, `Double` works out of the box with the default java implementation.
- stdlib `collection`: `ArrayDeque`, `ArrayList`, `HashMap`,`HashSet`, `LinkedHashSet`, `LinkedHashMap`.
- `ArrayList`, `HashMap`,`HashSet`, `LinkedHashSet`, `LinkedHashMap` works out of the box with the default java implementation.

Additional support is added for the following classes in kotlin:

- Unsigned primitives: `UByte`, `UShort`, `UInt`, `ULong`
- Empty collections: `emptyList`, `emptyMap`, `emptySet`
- Collections: `ArrayDeque`

Additional Notes:

- wrappers classes created from `withDefault` method is currently not supported.
