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

package org.apache.fory.serializer.collection;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.apache.fory.Fory;
import org.apache.fory.ForyTestBase;
import org.testng.Assert;
import org.testng.annotations.Test;

public class SubListSerializersTest extends ForyTestBase {
  @Test
  public void testSubListViewSerialization() {
    Fory fory = builder().withRefTracking(true).build();
    List<Integer> data = new ArrayList<>();
    Collections.addAll(data, 1, 2, 3, 4, 5, 6, 7);
    int length = fory.serialize(data).length;

    List<Integer> list1 = new ArrayList<>(data).subList(1, 3);
    Assert.assertTrue(fory.serialize(list1).length > length);
    // TODO parent list didn't copy modCount, but sublist does copy it.
    // Assert.assertEquals(fory.copy(list1), list1);

    List<Integer> list2 = new LinkedList<>(data).subList(1, 3);
    Assert.assertTrue(fory.serialize(list2).length > length);
    // TODO parent list didn't copy modCount, but sublist does copy it.
    // Assert.assertEquals(fory.copy(list2), list2);

    List<Integer> list3 = Collections.unmodifiableList(data).subList(1, 3);
    Assert.assertTrue(fory.serialize(list3).length > length);
    // TODO parent list didn't copy modCount, but sublist does copy it.
    // Assert.assertEquals(fory.copy(list3), list3);

    List<Integer> list4 = Collections.synchronizedList(data).subList(1, 3);
    Assert.assertTrue(fory.serialize(list4).length > length);
    // TODO parent list didn't copy modCount, but sublist does copy it.
    // Assert.assertEquals(fory.copy(list4), list4);

    List<Object> list5 = Arrays.asList(data.toArray()).subList(1, 3);
    Assert.assertTrue(fory.serialize(list5).length > length);
    // TODO parent list didn't copy modCount, but sublist does copy it.
    // Assert.assertEquals(fory.copy(list5), list5);
  }

  @Test
  public void testSubListNoViewSerialization() {
    Fory fory = builder().withRefTracking(false).build();
    List<Integer> data = new ArrayList<>();
    Collections.addAll(data, 1, 2, 3, 4, 5, 6, 7);
    int length = fory.serialize(data).length;

    List<Integer> list1 = new ArrayList<>(data).subList(1, 3);
    Assert.assertTrue(fory.serialize(list1).length < length);
    Assert.assertEquals(fory.copy(list1), list1);

    List<Integer> list2 = new LinkedList<>(data).subList(1, 3);
    Assert.assertTrue(fory.serialize(list2).length < length);
    Assert.assertEquals(fory.copy(list2), list2);

    List<Integer> list3 = Collections.unmodifiableList(data).subList(1, 3);
    Assert.assertTrue(fory.serialize(list3).length < length);
    Assert.assertEquals(fory.copy(list3), list3);

    List<Integer> list4 = Collections.synchronizedList(data).subList(1, 3);
    Assert.assertTrue(fory.serialize(list4).length < length);
    Assert.assertEquals(fory.copy(list4), list4);

    List<Object> list5 = Arrays.asList(data.toArray()).subList(1, 3);
    Assert.assertTrue(fory.serialize(list5).length < length);
    Assert.assertEquals(fory.copy(list5), list5);
  }
}
