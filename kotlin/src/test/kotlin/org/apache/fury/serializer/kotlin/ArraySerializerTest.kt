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

package org.apache.fury.serializer.kotlin

import org.apache.fury.Fury
import org.apache.fury.config.Language
import org.testng.Assert.assertEquals
import org.testng.annotations.Test

@OptIn(ExperimentalUnsignedTypes::class)
class ArraySerializerTest {
  @Test
  fun testSimpleArray() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val array = arrayOf("Apple", "Banana", "Orange", "Pineapple")
    assertEquals(array, fury.deserialize(fury.serialize(array)))
  }

  @Test
  fun testMultidimensional() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val array = Array(2) { Array<Int>(2) { 0 } }
    assertEquals(array, fury.deserialize(fury.serialize(array)))
  }

  @Test
  fun testAnyArray() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val array = arrayOf<Any?>("Apple", 1, null, 3.141, 1.2f)
    assertEquals(array, fury.deserialize(fury.serialize(array)))
  }

  @Test
  fun testEmptyArray() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val array = emptyArray<Any?>()
    assertEquals(array, fury.deserialize(fury.serialize(array)))
  }

  @Test
  fun testBooleanArray() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val array = booleanArrayOf(true, false)
    assertEquals(array, fury.deserialize(fury.serialize(array)))
  }

  @Test
  fun testByteArray() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val array = byteArrayOf(0xFF.toByte(), 0xCA.toByte(), 0xFF.toByte())
    assertEquals(array, fury.deserialize(fury.serialize(array)))
  }

  @Test
  fun testCharArray() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val array = charArrayOf('a', 'b', 'c')
    assertEquals(array, fury.deserialize(fury.serialize(array)))
  }

  @Test
  fun testDoubleArray() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val array = doubleArrayOf(1.0, 2.0, 3.0)
    assertEquals(array, fury.deserialize(fury.serialize(array)))
  }

  @Test
  fun testFloatArray() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val array = floatArrayOf(1.0f, 2.0f)
    assertEquals(array, fury.deserialize(fury.serialize(array)))
  }

  @Test
  fun testIntArray() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val array = intArrayOf(1, 2, 3)
    assertEquals(array, fury.deserialize(fury.serialize(array)))
  }

  @Test
  fun testLongArray() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val array = longArrayOf(1L, 2L, 3L)
    assertEquals(array, fury.deserialize(fury.serialize(array)))
  }

  @Test
  fun testShortArray() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val array = shortArrayOf(1, 2, 3)
    assertEquals(array, fury.deserialize(fury.serialize(array)))
  }

  @Test
  fun testUByteArray() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val array = ubyteArrayOf(0xFFu, 0xEFu, 0x00u)
    assert(array.contentEquals(fury.deserialize(fury.serialize(array)) as UByteArray))
  }

  @Test
  fun testUShortArray() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val array = ushortArrayOf(1u, 2u)
    assert(array.contentEquals(fury.deserialize(fury.serialize(array)) as UShortArray))
  }

  @Test
  fun testUIntArray() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val array = uintArrayOf(1u, 2u)
    assert(array.contentEquals(fury.deserialize(fury.serialize(array)) as UIntArray))
  }

  @Test
  fun testULongArray() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val array = ulongArrayOf(1u, 2u, 3u)
    assert(array.contentEquals(fury.deserialize(fury.serialize(array)) as ULongArray))
  }
}
