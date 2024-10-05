package org.apache.fury.serializer.kotlin

interface CollectionBuilder <E, T: Iterable<E>> {
    fun add(element: E)
    fun result(): T
}

class ListBuilder<E> : CollectionBuilder<E, List<E>> {
    private val builder = mutableListOf<E>()
    override fun add(element: E) {
        builder.add(element)
    }
    override fun result(): List<E> = builder
}

class MutableListBuilder<E> : CollectionBuilder<E, MutableList<E>> {
    private val builder = mutableListOf<E>()
    override fun add(element: E) {
        builder.add(element)
    }

    override fun result(): MutableList<E> = builder
}

class ArrayDequeBuilder<E> : CollectionBuilder<E, ArrayDeque<E>> {
    private val builder = ArrayDeque<E>()
    override fun add(element: E) {
        builder.add(element)
    }

    override fun result(): ArrayDeque<E> = builder
}

class SetBuilder<E> : CollectionBuilder<E, Set<E>> {
    private val builder = mutableSetOf<E>()
    override fun add(element: E) {
        builder.add(element)
    }
    override fun result(): Set<E> = builder
}

class MutableSetBuilder<E> : CollectionBuilder<E, MutableSet<E>> {
    private val builder = mutableSetOf<E>()
    override fun add(element: E) {
        builder.add(element)
    }
    override fun result(): MutableSet<E> = builder
}
