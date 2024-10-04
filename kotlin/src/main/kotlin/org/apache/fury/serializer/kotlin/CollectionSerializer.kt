package org.apache.fury.serializer.kotlin

import org.apache.fury.Fury
import org.apache.fury.memory.MemoryBuffer
import org.apache.fury.serializer.collection.AbstractCollectionSerializer

abstract class AbstractKotlinCollectionSerializer<E, T: Iterable<E>>(
    fury: Fury,
    cls: Class<T>
) : AbstractCollectionSerializer<T>(fury, cls) {
    abstract override fun onCollectionWrite(buffer: MemoryBuffer, value: T?): Collection<E>?

    override fun read(buffer: MemoryBuffer): T? {
        val collection = newCollection(buffer)
        val numElements = getAndClearNumElements()
        if (numElements != 0) readElements(fury, buffer, collection, numElements)
        return onCollectionRead(collection)
    }

    override fun newCollection(buffer: MemoryBuffer?): Collection<E>? {
        TODO("Not yet implemented")
    }

    override fun onCollectionRead(collection: Collection<*>?): T? {
        TODO("Not yet implemented")
    }
}

typealias CollectionAdaptor<E> = java.util.AbstractCollection<E>

/**
 * An adapter which wraps a kotlin iterable into a [[java.util.Collection]].
 *
 *
 */
private class IterableAdaptor<E>(
    coll: Iterable<E>
) : CollectionAdaptor<E>() {
    private val mutableList = coll.toMutableList()

    override val size: Int
        get() = mutableList.count()

    override fun iterator(): MutableIterator<E> =
        mutableList.iterator()
}

/**
 * An adapter which wraps a kotlin set into a [[java.util.Collection]].
 *
 *
 */
private class SetAdaptor<E>(
    coll: Set<E>
) : CollectionAdaptor<E>() {
    private val mutableSet = coll.toMutableSet()

    override val size: Int
        get() = mutableSet.size

    override fun iterator(): MutableIterator<E> =
        mutableSet.iterator()
}
