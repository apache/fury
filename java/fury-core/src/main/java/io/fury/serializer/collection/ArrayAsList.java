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

package io.fury.serializer.collection;

import io.fury.annotation.Internal;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.Objects;
import java.util.RandomAccess;

/**
 * A List which wrap a Java array like `java.util.Arrays.ArrayList`, but allow to replace wrapped
 * array. Used for serialization only, do not use it in other scenarios.
 *
 * @author chaokunyang
 */
@Internal
public class ArrayAsList<E> extends AbstractList<E> implements RandomAccess, java.io.Serializable {
  private E[] array;
  private int size;

  @SuppressWarnings("unchecked")
  public ArrayAsList(int size) {
    array = (E[]) new Object[size];
  }

  public ArrayAsList(E[] array) {
    this.array = Objects.requireNonNull(array);
    size = array.length;
  }

  @Override
  public int size() {
    return size;
  }

  @Override
  public boolean add(E e) {
    array[size++] = e;
    return true;
  }

  @Override
  public E get(int index) {
    return array[index];
  }

  @Override
  public E set(int index, E element) {
    E prev = array[index];
    array[index] = element;
    return prev;
  }

  public void setArray(E[] a) {
    this.array = a;
  }

  public E[] getArray() {
    return array;
  }

  @Override
  public int indexOf(Object o) {
    E[] array = this.array;
    if (o == null) {
      for (int i = 0; i < array.length; i++) {
        if (array[i] == null) {
          return i;
        }
      }
    } else {
      for (int i = 0; i < array.length; i++) {
        if (o.equals(array[i])) {
          return i;
        }
      }
    }
    return -1;
  }

  @Override
  public boolean contains(Object o) {
    return indexOf(o) >= 0;
  }

  /** Returns original array without copy. */
  @Override
  public Object[] toArray() {
    return array;
  }

  @Override
  public Iterator<E> iterator() {
    return new Iterator<E>() {
      private int index;

      @Override
      public boolean hasNext() {
        return index < array.length;
      }

      @Override
      public E next() {
        return array[index++];
      }
    };
  }
}
