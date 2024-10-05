package org.apache.fury.serializer.kotlin

import org.apache.fury.Fury
import org.apache.fury.config.Language
import org.testng.Assert.assertEquals
import org.testng.annotations.Test

class CollectionSerializerTest {
    @Test
    fun testSerializeList() {
        val fury: Fury = Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .suppressClassRegistrationWarnings(false)
            .build()

        KotlinSerializers.registerSerializers(fury)

        val list = ArrayDeque(listOf(1,2,3,4,5))
        assertEquals (list, fury.deserialize(fury.serialize(list)))
    }
}
