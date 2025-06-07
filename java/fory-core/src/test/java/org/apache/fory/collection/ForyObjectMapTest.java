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

import java.util.HashMap;
import java.util.Map;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ForyObjectMapTest {

  @Test
  public void testIterable() {
    ForyObjectMap<String, String> map = new ObjectMap<>(4, 0.2f);
    Map<String, String> hashMap = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      map.put("k" + i, "v" + i);
      hashMap.put("k" + i, "v" + i);
    }
    Map<String, String> hashMap2 = new HashMap<>();
    for (Map.Entry<String, String> entry : map.iterable()) {
      hashMap2.put(entry.getKey(), entry.getValue());
    }
    Assert.assertEquals(hashMap2, hashMap);
  }

  @Test
  public void testForEach() {
    ForyObjectMap<String, String> map = new ObjectMap<>(4, 0.2f);
    Map<String, String> hashMap = new HashMap<>();
    for (int i = 0; i < 100; i++) {
      map.put("k" + i, "v" + i);
      hashMap.put("k" + i, "v" + i);
    }
    Map<String, String> hashMap2 = new HashMap<>();
    map.forEach(hashMap2::put);
    Assert.assertEquals(hashMap2, hashMap);
  }
}
