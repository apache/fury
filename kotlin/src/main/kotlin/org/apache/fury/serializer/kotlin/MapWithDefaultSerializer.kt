package org.apache.fury.serializer.kotlin

import org.apache.fury.Fury
import org.apache.fury.memory.MemoryBuffer
import org.apache.fury.serializer.collection.AbstractMapSerializer

class MapWithDefaultSerializer<K, V, T : Map<K, V>>(
    fury: Fury,
    cls: Class<T>
) : AbstractMapSerializer<T>(fury, cls) {
    override fun onMapWrite(buffer: MemoryBuffer, value: T): Map<*, *> {
        TODO("Not yet implemented")
    }

    override fun read(buffer: MemoryBuffer): T {
        TODO("Not yet implemented")
    }

    override fun onMapCopy(map: Map<*, *>): T {
        TODO("Not yet implemented")
    }

    override fun onMapRead(map: Map<*, *>): T {
        TODO("Not yet implemented")
    }
}
