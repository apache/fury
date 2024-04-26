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

package org.apache.fury.collection;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotSame;

import java.util.stream.IntStream;
import org.testng.annotations.Test;

public class ObjectArrayTest {

  @Test
  public void testObjectArray() {
    ObjectArray array = new ObjectArray(10);
    for (int i = 0; i < 10; i++) {
      array.add("abc");
    }
    assertEquals(array.objects, IntStream.range(0, 10).mapToObj(i -> "abc").toArray());
    Object[] elementData = array.objects;
    array.add(1);
    assertNotSame(elementData, array.objects);
  }

  @Test
  public void testClearObjectArray() {
    int[] numObjs = new int[] {100, 500, 1000, 5000, 10000, 100000, 1000000};
    for (int numObj : numObjs) {
      Object[] array = new Object[numObj];
      Object o = new Object();
      for (int i = 0; i < numObj; i++) {
        array[i] = o;
      }
      ObjectArray.clearObjectArray(array, 0, array.length);
      for (int i = 0; i < array.length; i++) {
        Object value = array[i];
        if (value != null) {
          throw new IllegalStateException(String.format("numObj: %d, index: %d", numObj, i));
        }
      }
      for (int i = 0; i < numObj; i++) {
        array[i] = o;
      }
      ObjectArray.clearObjectArray(array, 1, array.length - 1);
      for (int i = 1; i < array.length; i++) {
        Object value = array[i];
        if (value != null) {
          throw new IllegalStateException(String.format("numObj: %d, index: %d", numObj, i));
        }
      }
    }
  }
}
