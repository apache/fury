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

package org.apache.fory.collection;

import java.util.Arrays;

/** A resizable int array which avoids the boxing in {@code ArrayList<Integer>}. */
public class IntArray {
  public int[] elementData;
  public int size;

  public IntArray(int capacity) {
    elementData = new int[capacity];
  }

  public void add(int value) {
    int[] items = this.elementData;
    if (size == items.length) {
      int newSize = size * 2;
      int[] newItems = new int[newSize];
      System.arraycopy(items, 0, newItems, 0, items.length);
      this.elementData = newItems;
      items = newItems;
    }
    items[size++] = value;
  }

  public int get(int index) {
    if (index >= size) {
      throw new IndexOutOfBoundsException(String.valueOf(index));
    }
    return elementData[index];
  }

  /** Removes and returns the last item. */
  public int pop() {
    return elementData[--size];
  }

  public void clear() {
    size = 0;
  }

  public String toString() {
    return Arrays.toString(Arrays.copyOfRange(elementData, 0, size));
  }
}
