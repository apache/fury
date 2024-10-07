package org.apache.fury.serializer.kotlin

import org.apache.fury.Fury
import org.apache.fury.config.Language
import org.testng.Assert.assertEquals
import org.testng.annotations.Test

class CollectionSerializerTest {
    @Test
    fun testSerialize() {
        val fury: Fury = Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .requireClassRegistration(true)
            .suppressClassRegistrationWarnings(false)
            .build()

        KotlinSerializers.registerSerializers(fury)
        val arrayDeque = ArrayDeque(listOf(1,2,3,4,5))
        assertEquals (arrayDeque, fury.deserialize(fury.serialize(arrayDeque)))

        val arrayList = arrayListOf(1,2,3,4,5)
        assertEquals (arrayList, fury.deserialize(fury.serialize(arrayList)))

        val emptyList = listOf<Int>()
        assertEquals (emptyList, fury.deserialize(fury.serialize(emptyList)))

        val list = listOf(1,2,3,4,5)
        assertEquals (list, fury.deserialize(fury.serialize(list)))

        val mutableList = mutableListOf(1,2,3,4,5)
        assertEquals (mutableList, fury.deserialize(fury.serialize(mutableList)))

        val emptySet = setOf<Int>()
        assertEquals (emptySet, fury.deserialize(fury.serialize(emptySet)))

        val set = setOf(1,2,3,4,5)
        assertEquals (set, fury.deserialize(fury.serialize(set)))

        val mutableSet = mutableSetOf(1,2,3,4,5)
        assertEquals (mutableSet, fury.deserialize(fury.serialize(mutableSet)))

        val emptyMap: Map<Int, String> = mapOf()
        assertEquals (emptyMap, fury.deserialize(fury.serialize(emptyMap)))

        val map = mapOf(1 to "one",2 to "two",3 to "three")
        assertEquals (map, fury.deserialize(fury.serialize(map)))

        val mutableMap = mapOf(1 to "one",2 to "two",3 to "three")
        assertEquals (map, fury.deserialize(fury.serialize(mutableMap)))

        val mapWithDefault = mapOf(1 to "one",2 to "two",3 to "three").withDefault { it-> "unknown" }
        assertEquals (mapWithDefault, fury.deserialize(fury.serialize(mapWithDefault)))

        val mutableMapWithDefault = mutableMapOf(1 to "one",2 to "two",3 to "three").withDefault { it-> "unknown" }
        assertEquals (mutableMapWithDefault, fury.deserialize(fury.serialize(mutableMapWithDefault)))
    }
}
