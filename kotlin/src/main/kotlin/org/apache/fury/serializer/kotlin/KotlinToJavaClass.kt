package org.apache.fury.serializer.kotlin

object KotlinToJavaClass {
    val ArrayDequeClass = ArrayDeque::class.java
    val EmptyListClass = emptyList<Any>()::class.java
    val EmptySetClass = emptySet<Any>()::class.java
    val EmptyMapClass = emptyMap<Any, Any>()::class.java
}
