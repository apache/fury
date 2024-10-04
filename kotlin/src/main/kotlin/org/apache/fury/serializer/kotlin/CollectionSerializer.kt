package org.apache.fury.serializer.kotlin

import org.apache.fury.Fury
import org.apache.fury.memory.MemoryBuffer
import org.apache.fury.serializer.collection.AbstractCollectionSerializer

abstract class AbstractKotlinCollectionSerializer<A, T: Iterable<A>>(
    fury: Fury,
    cls: Class<T>
) : AbstractCollectionSerializer<T>(fury, cls) {
    abstract override fun onCollectionWrite(buffer: MemoryBuffer, value: T?): Collection<A>?

    override fun read(buffer: MemoryBuffer): T? {
        val collection = newCollection(buffer)
        val numElements = getAndClearNumElements()
        if (numElements != 0) readElements(fury, buffer, collection, numElements)
        return onCollectionRead(collection)
    }

    override fun newCollection(buffer: MemoryBuffer?): Collection<A>? {
        TODO("Not yet implemented")
    }

    override fun onCollectionRead(collection: Collection<*>?): T? {
        TODO("Not yet implemented")
    }
}

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
