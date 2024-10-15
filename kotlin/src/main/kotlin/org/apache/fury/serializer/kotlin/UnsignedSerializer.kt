package org.apache.fury.serializer.kotlin

import org.apache.fury.Fury
import org.apache.fury.memory.MemoryBuffer
import org.apache.fury.serializer.Serializers
import org.apache.fury.type.Type

class UByteSerializer(
    fury: Fury,
) : Serializers.CrossLanguageCompatibleSerializer<UByte>(
    fury,
    UByte::class.java,
    Type.UINT8.id,
    fury.isBasicTypesRefIgnored,
    true) {

    override fun write(buffer: MemoryBuffer, value: UByte) {
        buffer.writeVarUint32Small7(value.toInt())
    }

    override fun read(buffer: MemoryBuffer): UByte {
        return buffer.readVarUint32().toUByte()
    }
}

class UShortSerializer(
    fury: Fury,
) : Serializers.CrossLanguageCompatibleSerializer<UShort>(
    fury,
    UShort::class.java,
    Type.UINT16.id,
    fury.isBasicTypesRefIgnored,
    true
) {
    override fun write(buffer: MemoryBuffer, value: UShort) {
        buffer.writeVarUint32(value.toInt())
    }
    override fun read(buffer: MemoryBuffer): UShort {
        return buffer.readVarUint32().toUShort()
    }
}

class UIntSerializer(
    fury: Fury,
) : Serializers.CrossLanguageCompatibleSerializer<UInt>(
    fury,
    UInt::class.java,
    Type.UINT32.id,
    fury.isBasicTypesRefIgnored,
    true) {

    override fun write(buffer: MemoryBuffer, value: UInt) {
        buffer.writeVarUint32(value.toInt())
    }

    override fun read(buffer: MemoryBuffer): UInt {
        return buffer.readVarUint32().toUInt()
    }
}

class ULongSerializer(
    fury: Fury,
) : Serializers.CrossLanguageCompatibleSerializer<ULong>(
    fury,
    ULong::class.java,
    Type.UINT64.id,
    fury.isBasicTypesRefIgnored,
    true
) {
    override fun write(buffer: MemoryBuffer, value: ULong) {
        buffer.writeVarUint64(value.toLong())
    }
    override fun read(buffer: MemoryBuffer): ULong {
        return buffer.readVarUint64().toULong()
    }
}
