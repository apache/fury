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

import static org.testng.Assert.*;

import java.util.stream.IntStream;
import org.testng.annotations.Test;

public class IntArrayTest {

  @Test
  public void testIntArray() {
    IntArray array = new IntArray(10);
    for (int i = 0; i < 10; i++) {
      array.add(i * 2);
    }
    assertEquals(array.elementData, IntStream.range(0, 10).map(i -> i * 2).toArray());
    int[] elementData = array.elementData;
    array.add(1);
    assertNotSame(elementData, array.elementData);
  }
}
