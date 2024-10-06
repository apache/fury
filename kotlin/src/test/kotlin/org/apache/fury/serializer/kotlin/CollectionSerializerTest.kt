package org.apache.fury.serializer.kotlin

import org.apache.fury.Fury
import org.apache.fury.config.Language
import org.testng.Assert.assertEquals
import org.testng.annotations.Test

class CollectionSerializerTest {
    @Test
    fun testSerialize() {
        val fury: Fury = Fury.builder()
            .withLanguage(Language.XLANG)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .suppressClassRegistrationWarnings(false)
            .build()

        KotlinSerializers.registerSerializers(fury)
        val list = ArrayDeque(listOf(1,2,3,4,5))
        assertEquals (list, fury.deserialize(fury.serialize(list)))

        val list2 = listOf(1,2,3,4,5)
        assertEquals (list2, fury.deserialize(fury.serialize(list2)))

        val set = setOf(1,2,3,4,5)
        assertEquals (set, fury.deserialize(fury.serialize(set)))

        val map = mapOf(1 to "one",2 to "two",3 to "three")
        assertEquals (map, fury.deserialize(fury.serialize(map)))
    }
}
