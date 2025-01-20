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
import org.testng.Assert
import org.testng.annotations.Test

@ExperimentalUnsignedTypes
class UnsignedBoundarySerializerTests {
  @Test
  fun testUByteBoundarySerialization() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()

    KotlinSerializers.registerSerializers(fury)

    val boundaryMin = UByte.MIN_VALUE
    val boundaryMax = UByte.MAX_VALUE

    Assert.assertEquals(boundaryMin, fury.deserialize(fury.serialize(boundaryMin)))
    Assert.assertEquals(boundaryMax, fury.deserialize(fury.serialize(boundaryMax)))
  }

  @Test
  fun testUShortBoundarySerialization() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()

    KotlinSerializers.registerSerializers(fury)

    val boundaryMin = UShort.MIN_VALUE
    val boundaryMax = UShort.MAX_VALUE

    Assert.assertEquals(boundaryMin, fury.deserialize(fury.serialize(boundaryMin)))
    Assert.assertEquals(boundaryMax, fury.deserialize(fury.serialize(boundaryMax)))
  }

  @Test
  fun testUIntBoundarySerialization() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()

    KotlinSerializers.registerSerializers(fury)

    val boundaryMin = UInt.MIN_VALUE
    val boundaryMax = UInt.MAX_VALUE

    Assert.assertEquals(boundaryMin, fury.deserialize(fury.serialize(boundaryMin)))
    Assert.assertEquals(boundaryMax, fury.deserialize(fury.serialize(boundaryMax)))
  }

  @Test
  fun testULongBoundarySerialization() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()

    KotlinSerializers.registerSerializers(fury)

    val boundaryMin = ULong.MIN_VALUE
    val boundaryMax = ULong.MAX_VALUE

    Assert.assertEquals(boundaryMin, fury.deserialize(fury.serialize(boundaryMin)))
    Assert.assertEquals(boundaryMax, fury.deserialize(fury.serialize(boundaryMax)))
  }

  @Test
  fun testUByteArrayBoundarySerialization() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()

    KotlinSerializers.registerSerializers(fury)

    val array = ubyteArrayOf(UByte.MIN_VALUE, UByte.MAX_VALUE)

    assert(array.contentEquals(fury.deserialize(fury.serialize(array)) as UByteArray))
  }

  @Test
  fun testUShortArrayBoundarySerialization() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()

    KotlinSerializers.registerSerializers(fury)

    val array = ushortArrayOf(UShort.MIN_VALUE, UShort.MAX_VALUE)

    assert(array.contentEquals(fury.deserialize(fury.serialize(array)) as UShortArray))
  }

  @Test
  fun testUIntArrayBoundarySerialization() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()

    KotlinSerializers.registerSerializers(fury)

    val array = uintArrayOf(UInt.MIN_VALUE, UInt.MAX_VALUE)

    assert(array.contentEquals(fury.deserialize(fury.serialize(array)) as UIntArray))
  }

  @Test
  fun testULongArrayBoundarySerialization() {
    val fury: Fury =
      Fury.builder().withLanguage(Language.JAVA).requireClassRegistration(true).build()

    KotlinSerializers.registerSerializers(fury)

    val array = ulongArrayOf(ULong.MIN_VALUE, ULong.MAX_VALUE)

    assert(array.contentEquals(fury.deserialize(fury.serialize(array)) as ULongArray))
  }
}
