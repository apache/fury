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

import kotlin.random.Random
import kotlin.test.Test
import kotlin.time.Duration
import kotlin.time.Duration.Companion.days
import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.microseconds
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.nanoseconds
import kotlin.time.Duration.Companion.seconds
import kotlin.uuid.ExperimentalUuidApi
import kotlin.uuid.Uuid
import org.apache.fury.Fury
import org.apache.fury.config.Language
import org.testng.Assert

class BuiltinClassSerializerTests {
  @Test
  fun testSerializePair() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()

    KotlinSerializers.registerSerializers(fury)
    val value = Pair(1, "one")
    Assert.assertEquals(value, fury.deserialize(fury.serialize(value)))
  }

  @Test
  fun testSerializeTriple() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()

    KotlinSerializers.registerSerializers(fury)
    val value = Triple(1, "one", null)
    Assert.assertEquals(value, fury.deserialize(fury.serialize(value)))
  }

  @Suppress("UNCHECKED_CAST")
  @Test
  fun testSerializeResult() {
    val fury: Fury =
      Fury.builder()
        .withLanguage(Language.JAVA)
        .requireClassRegistration(true)
        .withRefTracking(true)
        .build()

    KotlinSerializers.registerSerializers(fury)
    val value1 = Result.success(5)
    Assert.assertEquals(value1, fury.deserialize(fury.serialize(value1)))

    val value2 = Result.failure<Int>(IllegalStateException("my exception"))
    val roundtrip = fury.deserialize(fury.serialize(value2)) as Result<Int>
    Assert.assertTrue(roundtrip.isFailure)
    Assert.assertEquals(roundtrip.exceptionOrNull()!!.message, "my exception")
  }

  @Test
  fun testSerializeRanges() {
    val fury: Fury =
      Fury.builder()
        .withLanguage(Language.JAVA)
        .requireClassRegistration(true)
        .withRefTracking(true)
        .build()

    KotlinSerializers.registerSerializers(fury)
    val value1 = 1..4
    Assert.assertEquals(value1, fury.deserialize(fury.serialize(value1)))

    val value2 = 4 downTo 1
    Assert.assertEquals(value2, fury.deserialize(fury.serialize(value2)))

    val value3 = 0 ..< 8 step 2
    Assert.assertEquals(value3, fury.deserialize(fury.serialize(value3)))

    val value4 = 'a'..'d'
    Assert.assertEquals(value4, fury.deserialize(fury.serialize(value4)))

    val value5 = 'c' downTo 'a'
    Assert.assertEquals(value5, fury.deserialize(fury.serialize(value5)))

    val value6 = 0L..10L
    Assert.assertEquals(value6, fury.deserialize(fury.serialize(value6)))

    val value7 = 4L downTo 0L
    Assert.assertEquals(value7, fury.deserialize(fury.serialize(value7)))

    val value8 = 0u..4u
    Assert.assertEquals(value8, fury.deserialize(fury.serialize(value8)))

    val value9 = 4u downTo 0u
    Assert.assertEquals(value9, fury.deserialize(fury.serialize(value9)))

    val value10 = 0uL..4uL
    Assert.assertEquals(value10, fury.deserialize(fury.serialize(value10)))

    val value11 = 4uL downTo 0uL
    Assert.assertEquals(value11, fury.deserialize(fury.serialize(value11)))
  }

  @Test
  fun testSerializeRandom() {
    val fury: Fury =
      Fury.builder()
        .withLanguage(Language.JAVA)
        .requireClassRegistration(true)
        .withRefTracking(true)
        .build()

    KotlinSerializers.registerSerializers(fury)
    val value1 = Random(123)
    val roundtrip1 = fury.deserialize(fury.serialize(value1)) as Random

    Assert.assertEquals(roundtrip1.nextInt(), value1.nextInt())

    // The default random object will be roundtripped to the platform default random singleton
    // object.
    val value2 = Random.Default
    val roundtrip2 = fury.deserialize(fury.serialize(value2)) as Random

    Assert.assertEquals(roundtrip2, value2)
  }

  @Test
  fun testSerializeDuration() {
    val fury: Fury =
      Fury.builder()
        .withLanguage(Language.JAVA)
        .requireClassRegistration(true)
        .withRefTracking(true)
        .build()

    KotlinSerializers.registerSerializers(fury)

    val value1 = Duration.ZERO
    Assert.assertEquals(value1, fury.deserialize(fury.serialize(value1)))

    val value2 = Duration.INFINITE
    Assert.assertEquals(value2, fury.deserialize(fury.serialize(value2)))

    val value3 = -Duration.INFINITE
    Assert.assertEquals(value3, fury.deserialize(fury.serialize(value3)))

    val value4 = 1.nanoseconds
    Assert.assertEquals(value4, fury.deserialize(fury.serialize(value4)))

    val value5 = 1.microseconds
    Assert.assertEquals(value5, fury.deserialize(fury.serialize(value5)))

    val value6 = 1.milliseconds
    Assert.assertEquals(value6, fury.deserialize(fury.serialize(value6)))

    val value7 = 3.141.milliseconds
    Assert.assertEquals(value7, fury.deserialize(fury.serialize(value7)))

    val value8 = 1.seconds
    Assert.assertEquals(value8, fury.deserialize(fury.serialize(value8)))

    val value9 = 3.141.seconds
    Assert.assertEquals(value9, fury.deserialize(fury.serialize(value9)))

    val value10 = 5.minutes
    Assert.assertEquals(value10, fury.deserialize(fury.serialize(value10)))

    val value11 = 60.minutes
    Assert.assertEquals(value11, fury.deserialize(fury.serialize(value11)))

    val value12 = 12.hours
    Assert.assertEquals(value12, fury.deserialize(fury.serialize(value12)))

    val value13 = 13.days
    Assert.assertEquals(value13, fury.deserialize(fury.serialize(value13)))

    val value14 = 356.days
    Assert.assertEquals(value14, fury.deserialize(fury.serialize(value14)))
  }

  @OptIn(ExperimentalUuidApi::class)
  @Test
  fun testSerializeUuid() {
    val fury: Fury =
      Fury.builder()
        .withLanguage(Language.JAVA)
        .requireClassRegistration(true)
        .withRefTracking(true)
        .build()

    KotlinSerializers.registerSerializers(fury)

    val value = Uuid.fromLongs(1234L, 56789L)
    Assert.assertEquals(value, fury.deserialize(fury.serialize(value)))
  }

  @Test
  fun testSerializeRegex() {
    val fury: Fury =
      Fury.builder()
        .withLanguage(Language.JAVA)
        .requireClassRegistration(true)
        .withRefTracking(true)
        .build()

    KotlinSerializers.registerSerializers(fury)

    val value = Regex("12345")
    Assert.assertEquals(value.pattern, fury.deserialize(fury.serialize(value.pattern)))
    Assert.assertEquals(value.options, fury.deserialize(fury.serialize(value.options)))
  }
}
