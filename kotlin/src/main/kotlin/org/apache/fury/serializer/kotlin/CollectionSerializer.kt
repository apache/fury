package org.apache.fury.serializer.kotlin

import org.apache.fury.Fury
import org.apache.fury.serializer.collection.AbstractCollectionSerializer

abstract class AbstractKotlinCollectionSerializer<A, T: Iterable<A>>(
    fury: Fury,
    cls: Class<T>
) : AbstractCollectionSerializer<T>(fury, cls)

/**
 * A Collection adapter which wraps a kotlin iterable into a [[java.util.Collection]].
 *
 *
 */
private class CollectionAdaptor<A>(
    val coll: Iterable<A>
) : java.util.AbstractCollection<A>() {
    private val mutableList = coll.toMutableList()

    override val size: Int
        get() = coll.count()

    override fun iterator(): MutableIterator<A> =
        mutableList.iterator()
}
