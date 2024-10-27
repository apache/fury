/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

@file:OptIn(ExperimentalUnsignedTypes::class)

package org.apache.fury.serializer.kotlin

import org.apache.fury.Fury
import org.apache.fury.memory.MemoryBuffer
import org.apache.fury.serializer.Serializer
import org.apache.fury.type.Type

public abstract class AbstractDelegatingArraySerializer<T, T_Delegate>(
  fury: Fury,
  cls: Class<T>,
  private val delegateClass: Class<T_Delegate>
) : Serializer<T>(fury, cls) {

  // Lazily initialize the delegatingSerializer here to avoid lookup cost.
  private val delegatingSerializer by lazy { fury.classResolver.getSerializer(delegateClass) }

  protected abstract fun toDelegateClass(value: T): T_Delegate

  protected abstract fun fromDelegateClass(value: T_Delegate): T

  override fun getXtypeId(): Short {
    return (-Type.LIST.id).toShort()
  }

  override fun xwrite(buffer: MemoryBuffer, value: T) {
    write(buffer, value)
  }

  override fun xread(buffer: MemoryBuffer): T {
    return read(buffer)
  }

  override fun write(buffer: MemoryBuffer, value: T) {
    delegatingSerializer.write(buffer, toDelegateClass(value))
  }

  override fun read(buffer: MemoryBuffer): T {
    val delegatedValue = delegatingSerializer.read(buffer)
    return fromDelegateClass(delegatedValue)
  }
}

public class UByteArraySerializer(
  fury: Fury,
) :
  AbstractDelegatingArraySerializer<UByteArray, ByteArray>(
    fury,
    UByteArray::class.java,
    ByteArray::class.java
  ) {
  override fun toDelegateClass(value: UByteArray): ByteArray = value.toByteArray()

  override fun fromDelegateClass(value: ByteArray): UByteArray = value.toUByteArray()

  override fun copy(value: UByteArray): UByteArray = value.copyOf()
}

public class UShortArraySerializer(
  fury: Fury,
) :
  AbstractDelegatingArraySerializer<UShortArray, ShortArray>(
    fury,
    UShortArray::class.java,
    ShortArray::class.java
  ) {
  override fun toDelegateClass(value: UShortArray): ShortArray = value.toShortArray()

  override fun fromDelegateClass(value: ShortArray): UShortArray = value.toUShortArray()

  override fun copy(value: UShortArray): UShortArray = value.copyOf()
}

public class UIntArraySerializer(
  fury: Fury,
) :
  AbstractDelegatingArraySerializer<UIntArray, IntArray>(
    fury,
    UIntArray::class.java,
    IntArray::class.java
  ) {
  override fun toDelegateClass(value: UIntArray): IntArray = value.toIntArray()

  override fun fromDelegateClass(value: IntArray): UIntArray = value.toUIntArray()

  override fun copy(value: UIntArray): UIntArray = value.copyOf()
}

public class ULongArraySerializer(
  fury: Fury,
) :
  AbstractDelegatingArraySerializer<ULongArray, LongArray>(
    fury,
    ULongArray::class.java,
    LongArray::class.java
  ) {
  override fun toDelegateClass(value: ULongArray): LongArray = value.toLongArray()

  override fun fromDelegateClass(value: LongArray): ULongArray = value.toULongArray()

  override fun copy(value: ULongArray): ULongArray = value.copyOf()
}
