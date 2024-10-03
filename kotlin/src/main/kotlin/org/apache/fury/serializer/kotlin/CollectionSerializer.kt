package org.apache.fury.serializer.kotlin

import org.apache.fury.Fury
import org.apache.fury.serializer.collection.AbstractCollectionSerializer

abstract class AbstractKotlinCollectionSerializer<A, T: Iterable<A>>(
    fury: Fury,
    cls: Class<T?>?
) : AbstractCollectionSerializer<T>(fury, cls)
