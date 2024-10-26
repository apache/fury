# Apache Fury™ Kotlin

This provides additional Fury support for Kotlin Serialization on JVM:

Most standard kotlin types are already supported out of the box with the default Fury java implementation.

Fury Kotlin provides additional tests and implementation support for Kotlin types.

Fury Kotlin is tested and works with the following types:

- primitives: `Byte`, `Boolean`, `Int`, `Short`, `Long`, `Char`, `Float`, `Double`, `UByte`, `UShort`, `UInt`, `ULong`.
- `Byte`, `Boolean`, `Int`, `Short`, `Long`, `Char`, `Float`, `Double` works out of the box with the default fury java implementation.
- stdlib `collection`: `ArrayDeque`, `ArrayList`, `HashMap`,`HashSet`, `LinkedHashSet`, `LinkedHashMap`.
- `ArrayList`, `HashMap`,`HashSet`, `LinkedHashSet`, `LinkedHashMap` works out of the box with the default fury java implementation.
- `String` works out of the box with the default fury java implementation.
- arrays: `Array`, `BooleanArray`, `ByteArray`, `CharArray`, `DoubleArray`, `FloatArray`, `IntArray`, `LongArray`, `ShortArray`
- all standard array types work out of the box with the default fury java implementation.
- unsigned arrays: `UByteArray`, `UShortArray`, `UIntArray`, `ULongArray`
- from stdlib: `Pair`, `Triple`, `Result`
- kotlin.random: `Random`
- kotlin.ranges: `CharRange`, `CharProgression`, `IntRange`, `IntProgression`, `LongRange`, `LongProgression`, `UintRange`, `UintProgression`, `ULongRange`, `ULongProgression`
- kotlin.text: `Regex`
- kotlin.time: `Duration`
- kotlin.uuid: `Uuid`

Additional support is added for the following classes in kotlin:

- Unsigned primitives: `UByte`, `UShort`, `UInt`, `ULong`
- Unsigned array types: `UByteArray`, `UShortArray`, `UIntArray`, `ULongArray`
- Empty collections: `emptyList`, `emptyMap`, `emptySet`
- Collections: `ArrayDeque`
- kotlin.time: `Duration`
- kotlin.uuid: `Uuid`

Additional Notes:

- wrappers classes created from `withDefault` method is currently not supported.

## Building Fury Kotlin

```bash
mvn clean
mvn -T10 compile
```
## Code Format

```bash
mvn -T10 spotless:apply
```

## Testing

```bash
mvn -T10 test
```
