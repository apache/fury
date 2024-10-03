package org.apache.fury.serializer.kotlin

import org.apache.fury.Fury
import org.apache.fury.serializer.collection.AbstractMapSerializer

abstract class AbstractKotlinMapSerializer<K, V, T : Map<K, V>>(
    fury: Fury,
    cls: Class<T?>?
) : AbstractMapSerializer<T>(fury, cls)
