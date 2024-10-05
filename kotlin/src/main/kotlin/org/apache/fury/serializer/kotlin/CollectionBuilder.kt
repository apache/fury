package org.apache.fury.serializer.kotlin

interface CollectionBuilder <E, T: Iterable<E>> {
    fun add(element: E)
    fun result(): T
}

abstract class AbstractListBuilder<E, T:List<E>>(
    protected open val builder: MutableList<E>,
) : CollectionBuilder<E, T> {
    override fun add(element: E) {
        builder.add(element)
    }
}

abstract class AbstractSetBuilder<E, T: Set<E>>(
    protected open val builder: MutableSet<E>
) : CollectionBuilder<E, T> {
    override fun add(element: E) {
        builder.add(element)
    }
}

class ListBuilder<E>(
    builder: MutableList<E> = mutableListOf()
) : AbstractListBuilder<E, MutableList<E>>(builder) {
    override fun result(): MutableList<E> = builder
}

class ArrayDequeBuilder<E>(
    override val builder: ArrayDeque<E> = ArrayDeque()
) : AbstractListBuilder<E, ArrayDeque<E>>(builder) {
    override fun result(): ArrayDeque<E> = builder
}

class SetBuilder<E>(
    builder: MutableSet<E> = mutableSetOf()
) : AbstractSetBuilder<E, MutableSet<E>>(builder) {
    override fun result(): MutableSet<E> = builder
}
