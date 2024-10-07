package org.apache.fury.serializer.kotlin;

import org.apache.fury.Fury;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.serializer.SerializerFactory;

/**
 * Serializer dispatcher for kotlin types.
 */
@SuppressWarnings({"rawtypes", "unchecked"})
public class KotlinDispatcher implements SerializerFactory {
    @Override
    public Serializer createSerializer(Fury fury, Class<?> clz) {
        Class mapWithDefaultClass = KotlinToJavaClass.INSTANCE.getMapWithDefaultClass();
        Class mutableMapWithDefaultClass = KotlinToJavaClass.INSTANCE.getMutableMapWitDefaultClass();

        if (mapWithDefaultClass.isAssignableFrom(clz) ||
        mutableMapWithDefaultClass.isAssignableFrom(clz)) {
            return new MapWithDefaultSerializer(fury, clz);
        }

        return null;
    }
}
