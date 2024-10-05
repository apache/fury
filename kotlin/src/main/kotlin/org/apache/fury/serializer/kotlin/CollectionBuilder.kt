package org.apache.fury.serializer.kotlin

interface CollectionBuilder <E, T: Iterable<E>> {
    fun add(element: E): Boolean
    fun addAll(elements:Iterable<E>) : Boolean
    fun result(): T
}

abstract class AbstractListBuilder<E, T:List<E>>
    : CollectionBuilder<E, T>, CollectionAdapter<E>() {
    protected open val builder: MutableList<E> = mutableListOf()
    override val size: Int
        get() = builder.size
    override fun add(element: E) =
        builder.add(element)
    override fun addAll(elements: Iterable<E>): Boolean =
        builder.addAll(elements)
    override fun iterator(): MutableIterator<E> = builder.iterator()
}

abstract class AbstractSetBuilder<E, T: Set<E>>
    : CollectionBuilder<E, T>, CollectionAdapter<E>() {
    protected open val builder: MutableSet<E> = mutableSetOf()
    override val size: Int
        get() = builder.size
    override fun add(element: E) =
        builder.add(element)
    override fun addAll(elements: Iterable<E>): Boolean =
        builder.addAll(elements)
    override fun iterator(): MutableIterator<E> = builder.iterator()
}

class ListBuilder<E>: AbstractListBuilder<E, MutableList<E>>() {
    override fun result(): MutableList<E> = builder
}

class ArrayDequeBuilder<E>(
    override val builder: ArrayDeque<E>
): AbstractListBuilder<E, ArrayDeque<E>>() {
    override fun result(): ArrayDeque<E> = builder
}

class SetBuilder<E> : AbstractSetBuilder<E, MutableSet<E>>() {
    override fun result(): MutableSet<E> = builder
}
