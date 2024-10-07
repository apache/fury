package org.apache.fury.serializer.kotlin

object KotlinToJavaClass {
    val ArrayDequeClass = ArrayDeque::class.java
    val EmptyListClass = emptyList<Any>()::class.java
    val EmptySetClass = emptySet<Any>()::class.java
    val EmptyMapClass = emptyMap<Any, Any>()::class.java
    val MapWithDefaultClass = emptyMap<Any, Any>().withDefault { it -> it }::class.java
    val MutableMapWitDefaultClass = mutableMapOf<Any, Any>().withDefault { it -> it }::class.java
}
