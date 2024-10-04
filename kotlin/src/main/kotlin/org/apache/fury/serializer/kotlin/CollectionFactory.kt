package org.apache.fury.serializer.kotlin

interface CollectionFactory <E, T: Iterable<E>> {
    fun add(element: E)
    fun create(): T
}

class ListFactory<E> : CollectionFactory<E, List<E>> {
    private val builder = mutableListOf<E>()
    override fun add(element: E) {
        builder.add(element)
    }
    override fun create(): List<E> = builder
}

class MutableListFactory<E> : CollectionFactory<E, MutableList<E>> {
    private val builder = mutableListOf<E>()
    override fun add(element: E) {
        builder.add(element)
    }

    override fun create(): MutableList<E> = builder
}

class ArrayDequeFactory<E> : CollectionFactory<E, ArrayDeque<E>> {
    private val builder = ArrayDeque<E>()
    override fun add(element: E) {
        builder.add(element)
    }

    override fun create(): ArrayDeque<E> = builder
}

class SetFactory<E> : CollectionFactory<E, Set<E>> {
    private val builder = mutableSetOf<E>()
    override fun add(element: E) {
        builder.add(element)
    }
    override fun create(): Set<E> = builder
}

class MutableSetFactory<E> : CollectionFactory<E, MutableSet<E>> {
    private val builder = mutableSetOf<E>()
    override fun add(element: E) {
        builder.add(element)
    }
    override fun create(): MutableSet<E> = builder
}
