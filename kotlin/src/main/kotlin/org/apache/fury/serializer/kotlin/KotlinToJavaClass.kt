package org.apache.fury.serializer.kotlin

object KotlinToJavaClass {
    val ArrayDequeClass = ArrayDeque::class.java
    val EmptyListClass = emptyList<Any>().javaClass
    val EmptySetClass = emptySet<Any>().javaClass
    val EmptyMapClass = emptyMap<Any, Any>().javaClass
}
