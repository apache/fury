# Apache Fory™ Kotlin

This provides additional Fory support for Kotlin Serialization on JVM:

Most standard kotlin types are already supported out of the box with the default Fory java implementation.

Fory Kotlin provides additional tests and implementation support for Kotlin types.

Fory Kotlin is tested and works with the following types:

- primitives: `Byte`, `Boolean`, `Int`, `Short`, `Long`, `Char`, `Float`, `Double`, `UByte`, `UShort`, `UInt`, `ULong`.
- `Byte`, `Boolean`, `Int`, `Short`, `Long`, `Char`, `Float`, `Double` works out of the box with the default fory java implementation.
- stdlib `collection`: `ArrayDeque`, `ArrayList`, `HashMap`,`HashSet`, `LinkedHashSet`, `LinkedHashMap`.
- `ArrayList`, `HashMap`,`HashSet`, `LinkedHashSet`, `LinkedHashMap` works out of the box with the default fory java implementation.
- `String` works out of the box with the default fory java implementation.
- arrays: `Array`, `BooleanArray`, `ByteArray`, `CharArray`, `DoubleArray`, `FloatArray`, `IntArray`, `LongArray`, `ShortArray`
- all standard array types work out of the box with the default fory java implementation.
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

## Quick Start

```kotlin
import org.apache.fory.Fory
import org.apache.fory.ThreadSafeFory
import org.apache.fory.serializer.kotlin.KotlinSerializers

data class Person(val name: String, val id: Long, val github: String)
data class Point(val x : Int, val y : Int, val z : Int)

fun main(args: Array<String>) {
    // Note: following fory init code should be executed only once in a global scope instead
    // of initializing it everytime when serialization.
    val fory: ThreadSafeFory = Fory.builder().requireClassRegistration(true).buildThreadSafeFory()
    KotlinSerializers.registerSerializers(fory)
    fory.register(Person::class.java)
    fory.register(Point::class.java)

    val p = Person("Shawn Yang", 1, "https://github.com/chaokunyang")
    println(fory.deserialize(fory.serialize(p)))
    println(fory.deserialize(fory.serialize(Point(1, 2, 3))))
}
```

## Building Fory Kotlin

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
