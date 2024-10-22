package org.apache.fury.serializer.kotlin

import org.apache.fury.Fury
import org.apache.fury.memory.MemoryBuffer
import org.apache.fury.serializer.ImmutableSerializer
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid

@OptIn(ExperimentalUuidApi::class)
class UuidSerializer(
    fury: Fury
) : ImmutableSerializer<Uuid>(fury, Uuid::class.java) {
    override fun write(buffer: MemoryBuffer, value: Uuid) {
        value.toLongs { msb, lsb ->
            buffer.writeInt64(msb)
            buffer.writeInt64(lsb)
        }
    }

    override fun read(buffer: MemoryBuffer): Uuid {
        return Uuid.fromLongs(buffer.readInt64(), buffer.readInt64())
    }
}
