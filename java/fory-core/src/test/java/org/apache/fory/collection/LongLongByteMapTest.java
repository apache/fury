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

import org.testng.Assert;
import org.testng.annotations.Test;

public class LongLongByteMapTest {

  @Test
  public void testPut() {
    LongLongByteMap<String> map = new LongLongByteMap<>(10, 0.5f);
    map.put(1, 1, (byte) 1, "a");
    map.put(1, 2, (byte) 2, "b");
    map.put(1, 3, (byte) 3, "c");
    map.put(2, 1, (byte) 4, "d");
    map.put(3, 1, (byte) 5, "f");
    Assert.assertEquals(map.get(1, 1, (byte) 1), "a");
    Assert.assertEquals(map.get(1, 2, (byte) 2), "b");
    Assert.assertEquals(map.get(1, 3, (byte) 3), "c");
    Assert.assertEquals(map.get(2, 1, (byte) 4), "d");
    Assert.assertEquals(map.get(3, 1, (byte) 5), "f");
    for (int i = 1; i < 100; i++) {
      map.put(i, i, (byte) i, "a" + i);
      Assert.assertEquals(map.get(i, i, (byte) i), "a" + i);
    }
  }
}
