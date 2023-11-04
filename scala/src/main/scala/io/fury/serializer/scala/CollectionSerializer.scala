/*
 * Copyright 2023 The Fury Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.fury.serializer.scala

import io.fury.Fury
import io.fury.memory.MemoryBuffer
import io.fury.serializer.collection.AbstractCollectionSerializer

import java.util
import scala.collection.{Factory, Iterable, mutable}

/**
 * Serializer for scala collection.
 *
 * @author chaokunyang
 */
abstract class AbstractScalaCollectionSerializer[A, T <: Iterable[A]](fury: Fury, cls: Class[T])
  extends AbstractCollectionSerializer[T](fury, cls) {
  override def onCollectionWrite(buffer: MemoryBuffer, value: T): util.Collection[_]

  override def read(buffer: MemoryBuffer): T = {
    val collection = newCollection(buffer)
    if (numElements != 0) readElements(fury, buffer, collection, numElements)
    onCollectionRead(collection)
  }

  override def newCollection(buffer: MemoryBuffer): util.Collection[_] = {
    numElements = buffer.readPositiveVarInt()
    val factory = fury.readRef(buffer).asInstanceOf[Factory[A, T]]
    val builder = factory.newBuilder
    builder.sizeHint(numElements)
    new JavaCollectionBuilder[A, T](builder)
  }

  override def onCollectionRead(collection: util.Collection[_]): T = {
    collection.asInstanceOf[JavaCollectionBuilder[A, T]].builder.result()
  }
}

/**
 * A Iterable adapter to wrap scala iterable into a [[java.lang.Iterable]].
 *
 * @author chaokunyang
 */
private trait JavaIterable[A] extends java.lang.Iterable[A] {
  override def iterator(): util.Iterator[A] = new util.Iterator[A] {
    private val iterator = createIterator()

    override def hasNext: Boolean = iterator.hasNext

    override def next(): A = iterator.next
  }

  protected def createIterator(): Iterator[A]
}

/**
 * A Collection adapter which wrap scala iterable into a [[java.util.Collection]].
 *
 * @author chaokunyang
 */
private class CollectionAdapter[A, T](var coll: scala.collection.Iterable[A])
  extends util.AbstractCollection[A] with JavaIterable[A] {
  private var length: Int = -1

  override def size: Int = {
    if (length < 0) {
      length = coll.size
    }
    length
  }

  override protected def createIterator(): Iterator[A] = coll.iterator
}

/**
 * A List adapter which wrap scala Seq into a [[java.util.List]].
 *
 * @author chaokunyang
 */
private class ListAdapter[A](var coll: scala.collection.Seq[A])
  extends util.AbstractList[A] with JavaIterable[A] {
  override def get(index: Int): A = coll(index)

  override protected def createIterator(): Iterator[A] = coll.iterator

  override def size(): Int = coll.size
}

/**
 * A Collection adapter which build scala collection from elements.
 *
 * @author chaokunyang
 */
private class JavaCollectionBuilder[A, T](val builder: mutable.Builder[A, T])
  extends util.AbstractCollection[A] {
  override def add(e: A): Boolean = {
    builder.addOne(e)
    true
  }

  override def iterator(): util.Iterator[A] = ???

  override def size(): Int = ???
}

/**
 * Serializer for scala iterables.
 *
 * @author chaokunyang
 */
class ScalaCollectionSerializer[A, T <: Iterable[A]] (fury: Fury, cls: Class[T])
  extends AbstractScalaCollectionSerializer[A, T](fury, cls) {
  override def onCollectionWrite(buffer: MemoryBuffer, value: T): util.Collection[_] = {
    val factory: Factory[A, Any] = value.iterableFactory.iterableFactory
    val adapter = new CollectionAdapter[A, T](value)
    buffer.writePositiveVarInt(adapter.size)
    fury.writeRef(buffer, factory)
    adapter
  }
}

/**
 * Serializer for scala sorted set.
 *
 * @author chaokunyang
 */
class ScalaSortedSetSerializer[A, T <: scala.collection.SortedSet[A]](fury: Fury, cls: Class[T])
  extends AbstractScalaCollectionSerializer[A, T](fury, cls) {
  override def onCollectionWrite(buffer: MemoryBuffer, value: T): util.Collection[_] = {
    buffer.writePositiveVarInt(value.size)
    val factory = value.sortedIterableFactory.evidenceIterableFactory[Any](
      value.ordering.asInstanceOf[Ordering[Any]])
    fury.writeRef(buffer, factory)
    new CollectionAdapter[A, T](value)
  }
}

/**
 * Serializer for scala [[Seq]].
 *
 * @author chaokunyang
 */
class ScalaSeqSerializer[A, T <: scala.collection.Seq[A]](fury: Fury, cls: Class[T])
  extends AbstractScalaCollectionSerializer[A, T](fury, cls)  {
  override def onCollectionWrite(buffer: MemoryBuffer, value: T): util.Collection[_] = {
    buffer.writePositiveVarInt(value.size)
    val factory: Factory[A, Any] = value.iterableFactory.iterableFactory
    fury.writeRef(buffer, factory)
    new ListAdapter[Any](value)
  }
}
