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
import kotlin.collections.MutableMap.MutableEntry
import kotlin.test.Test

class BuiltinClassSerializerTests {
    @Test
    fun testSerializePair() {
        val fury: Fury = Fury.builder()
            .withLanguage(Language.JAVA)
            .requireClassRegistration(true)
            .build()

        KotlinSerializers.registerSerializers(fury)
        val value = Pair(1, "one")
        Assert.assertEquals(value, fury.deserialize(fury.serialize(value)))
    }

    @Test
    fun testSerializeTriple() {
        val fury: Fury = Fury.builder()
            .withLanguage(Language.JAVA)
            .requireClassRegistration(true)
            .build()

        KotlinSerializers.registerSerializers(fury)
        val value = Triple(1, "one", null)
        Assert.assertEquals(value, fury.deserialize(fury.serialize(value)))
    }

    @Suppress("UNCHECKED_CAST")
    @Test
    fun testSerializeResult() {
        val fury: Fury = Fury.builder()
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
}
