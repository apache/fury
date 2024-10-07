package org.apache.fury.serializer.kotlin

import org.apache.fury.Fury
import org.apache.fury.memory.MemoryBuffer
import org.apache.fury.serializer.Serializer

@Suppress("UNCHECKED_CAST")
class MapWithDefaultSerializer<K, V, T : Map<K, V>>(
    fury: Fury,
    cls: Class<T>
) : Serializer<T>(fury, cls) {

    override fun read(buffer: MemoryBuffer): T {
        val default = fury.readRef(buffer) as (K)->V
        val innerMap = fury.readRef(buffer) as Map<K, V>
        return innerMap.withDefault(default) as T
    }

    override fun write(buffer: MemoryBuffer, value: T) {
        val default = default(value)
        val innerMap = innerMap(value)

        fury.writeRef(buffer, default)
        fury.writeRef(buffer, innerMap)
    }

    private fun innerMap(value: T): Map<K, V> {
        val field = value.javaClass.getDeclaredField("map")
        field.isAccessible = true
        return field.get(value) as Map<K, V>
    }

    private fun default(value: Map<*, *>) : (K)-> V {
        val field = value.javaClass.getDeclaredField("default")
        field.isAccessible = true
        return field.get(value) as (K)->V
    }
}
