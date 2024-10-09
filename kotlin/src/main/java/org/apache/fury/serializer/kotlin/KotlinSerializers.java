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
package org.apache.fury.serializer.kotlin;

import org.apache.fury.Fury;
import org.apache.fury.resolver.ClassResolver;
import org.apache.fury.serializer.collection.CollectionSerializers;
import org.apache.fury.serializer.collection.MapSerializers;


/**
 * KotlinSerializers provide default serializers for kotlin.
 */
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
