package org.apache.fury.serializer.kotlin;

import org.apache.fury.Fury;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.serializer.SerializerFactory;

/**
 * Serializer dispatcher for kotlin types.
 */
@SuppressWarnings({"rawtypes"})
public class KotlinDispatcher implements SerializerFactory {
    @Override
    public Serializer createSerializer(Fury fury, Class<?> aClass) {
        return null;
    }
}
