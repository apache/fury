package org.apache.fury.serializer.kotlin;

import org.apache.fury.Fury;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.serializer.ArraySerializers;
import org.apache.fury.serializer.Serializer;
import org.apache.fury.serializer.SerializerFactory;
import org.apache.fury.serializer.collection.CollectionSerializers;
import org.apache.fury.serializer.collection.MapSerializers;


@SuppressWarnings({"rawtypes", "unchecked"})
public class KotlinSerializers {
    public static void registerSerializers(Fury fury) {
        ClassResolver resolver = setSerializerFactory(fury);

        // Arrays in kotlin map to java arrays.
        ArraySerializers.registerDefaultSerializers(fury);
        // Most kotlin stdlib implementation map to Java collections.
        CollectionSerializers.registerDefaultSerializers(fury);

        // EmptyList
        Class emptyListClass = KClassToJava.INSTANCE.getEmptyListClass();
        resolver.register(emptyListClass);
        resolver.registerSerializer(emptyListClass, new CollectionSerializers.EmptyListSerializer(fury, emptyListClass));

        // EmptySet
        Class emptySetClass = KClassToJava.INSTANCE.getEmptySetClass();
        resolver.register(emptySetClass);
        resolver.registerSerializer(emptySetClass, new CollectionSerializers.EmptySetSerializer(fury, emptySetClass));

        // EmptyMap
        Class emptyMapClass = KClassToJava.INSTANCE.getEmptyMapClass();
        resolver.register(emptyMapClass);
        resolver.registerSerializer(emptyMapClass, new MapSerializers.EmptyMapSerializer(fury, emptyMapClass));

        // Non-Java collection implementation in kotlin stdlib.
        Class arrayDequeClass = KClassToJava.INSTANCE.getArrayDequeClass();
        resolver.register(arrayDequeClass);
        resolver.registerSerializer(arrayDequeClass, new KotlinArrayDequeSerializer(fury, arrayDequeClass));
    }

    private static ClassResolver setSerializerFactory(Fury fury) {
        ClassResolver resolver = fury.getClassResolver();
        KotlinDispatcher dispatcher = new KotlinDispatcher();
        SerializerFactory factory = resolver.getSerializerFactory();
        if (factory != null) {
            SerializerFactory newFactory = (f, cls) -> {
                Serializer<?> serializer = factory.createSerializer(f, cls);
                if (serializer == null) {
                    serializer = dispatcher.createSerializer(f, cls);
                }
                return serializer;
            };
            resolver.setSerializerFactory(newFactory);
        } else {
            resolver.setSerializerFactory(dispatcher);
        }
        return resolver;
    }
}
