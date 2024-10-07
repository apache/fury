package org.apache.fury.serializer.kotlin;

import org.apache.fury.Fury;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.serializer.collection.CollectionSerializers;
import org.apache.fury.serializer.collection.MapSerializers;


@SuppressWarnings({"rawtypes", "unchecked"})
public class KotlinSerializers {
    public static void registerSerializers(Fury fury) {
        ClassResolver resolver = fury.getClassResolver();

        // EmptyList
        Class emptyListClass = KotlinToJavaClass.INSTANCE.getEmptyListClass();
        resolver.register(emptyListClass);
        resolver.registerSerializer(emptyListClass, new CollectionSerializers.EmptyListSerializer(fury, emptyListClass));

        // EmptySet
        Class emptySetClass = KotlinToJavaClass.INSTANCE.getEmptySetClass();
        resolver.register(emptySetClass);
        resolver.registerSerializer(emptySetClass, new CollectionSerializers.EmptySetSerializer(fury, emptySetClass));

        // EmptyMap
        Class emptyMapClass = KotlinToJavaClass.INSTANCE.getEmptyMapClass();
        resolver.register(emptyMapClass);
        resolver.registerSerializer(emptyMapClass, new MapSerializers.EmptyMapSerializer(fury, emptyMapClass));

        // Non-Java collection implementation in kotlin stdlib.
        Class arrayDequeClass = KotlinToJavaClass.INSTANCE.getArrayDequeClass();
        resolver.register(arrayDequeClass);
        resolver.registerSerializer(arrayDequeClass, new KotlinArrayDequeSerializer(fury, arrayDequeClass));
    }
}
