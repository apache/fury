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
package org.apache.fury.serializer.kotlin

interface CollectionBuilder <E, T: Iterable<E>> {
    fun add(element: E): Boolean
    fun result(): T
}

abstract class AbstractListBuilder<E, T:List<E>>
    : CollectionBuilder<E, T>, AdaptedCollection<E>() {
    protected open val builder: MutableList<E> = mutableListOf()
    override val size: Int
        get() = builder.size
    override fun add(element: E) =
        builder.add(element)
    override fun iterator(): MutableIterator<E> = builder.iterator()
}

class ArrayDequeBuilder<E>(
    override val builder: ArrayDeque<E>
): AbstractListBuilder<E, ArrayDeque<E>>() {
    override fun result(): ArrayDeque<E> = builder
}
