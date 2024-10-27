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

class CollectionSerializerTest {
  @Test
  fun testSerializeArrayDeque() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val arrayDeque = ArrayDeque(listOf(1, 2, 3, 4, 5))
    assertEquals(arrayDeque, fury.deserialize(fury.serialize(arrayDeque)))
  }

  @Test
  fun testSerializeArrayList() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()

    KotlinSerializers.registerSerializers(fury)
    val arrayList = arrayListOf(1, 2, 3, 4, 5)
    assertEquals(arrayList, fury.deserialize(fury.serialize(arrayList)))
  }

  @Test
  fun testSerializeEmptyList() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val emptyList = listOf<Int>()
    assertEquals(emptyList, fury.deserialize(fury.serialize(emptyList)))
  }

  @Test
  fun testSerializeList() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val list = listOf(1, 2, 3, 4, 5)
    assertEquals(list, fury.deserialize(fury.serialize(list)))
  }

  @Test
  fun testSerializeMutableList() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val mutableList = mutableListOf(1, 2, 3, 4, 5)
    assertEquals(mutableList, fury.deserialize(fury.serialize(mutableList)))
  }

  @Test
  fun testSerializeEmptySet() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val emptySet = setOf<Int>()
    assertEquals(emptySet, fury.deserialize(fury.serialize(emptySet)))
  }

  @Test
  fun testSerializeSet() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val set = setOf(1, 2, 3, 4, 5)
    assertEquals(set, fury.deserialize(fury.serialize(set)))
  }

  @Test
  fun testSerializeMutableSet() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val mutableSet = mutableSetOf(1, 2, 3, 4, 5)
    assertEquals(mutableSet, fury.deserialize(fury.serialize(mutableSet)))
  }

  @Test
  fun testSerializeHashSet() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val set = hashSetOf(1, 2, 3, 4, 5)
    assertEquals(set, fury.deserialize(fury.serialize(set)))
  }

  @Test
  fun testSerializeLinkedSet() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val set = linkedSetOf(1, 2, 3, 4, 5)
    assertEquals(set, fury.deserialize(fury.serialize(set)))
  }

  @Test
  fun testSerializeEmptyMap() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val emptyMap: Map<Int, String> = mapOf()
    assertEquals(emptyMap, fury.deserialize(fury.serialize(emptyMap)))
  }

  @Test
  fun testSerializeMap() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val map = mapOf(1 to "one", 2 to "two", 3 to "three")
    assertEquals(map, fury.deserialize(fury.serialize(map)))
  }

  @Test
  fun testSerializeMutableMap() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val mutableMap = mapOf(1 to "one", 2 to "two", 3 to "three")
    assertEquals(mutableMap, fury.deserialize(fury.serialize(mutableMap)))
  }

  @Test
  fun testSerializeHashMap() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val map = hashMapOf(1 to "one", 2 to "two", 3 to "three")
    assertEquals(map, fury.deserialize(fury.serialize(map)))
  }

  @Test
  fun testSerializeLinkedMap() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()
    KotlinSerializers.registerSerializers(fury)

    val map = linkedMapOf(1 to "one", 2 to "two", 3 to "three")
    assertEquals(map, fury.deserialize(fury.serialize(map)))
  }
}
