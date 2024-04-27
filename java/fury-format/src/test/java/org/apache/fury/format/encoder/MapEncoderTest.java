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

package org.apache.fury.format.encoder;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.fury.format.row.binary.BinaryMap;
import org.apache.fury.reflect.TypeRef;
import org.testng.Assert;
import org.testng.annotations.Test;

public class MapEncoderTest {

  @Test
  public void testMapEncoder() {
    Map<String, RowEncoderTest.Bar> bars = new HashMap<>();
    for (int k = 0; k < 5; k++) {
      RowEncoderTest.Bar bar = new RowEncoderTest.Bar();
      bar.f1 = k;
      bar.f2 = "i" + k;
      bars.put(bar.f2, bar);
    }

    MapEncoder<Map<String, RowEncoderTest.Bar>> encoder =
        Encoders.mapEncoder(bars.getClass(), String.class, RowEncoderTest.Bar.class);
    BinaryMap array = encoder.toMap(bars);
    Map<String, RowEncoderTest.Bar> newBars = encoder.fromMap(array);

    Assert.assertEquals(bars, newBars);

    byte[] bytes = encoder.encode(bars);
    Map<String, RowEncoderTest.Bar> decodeMap = encoder.decode(bytes);
    Assert.assertEquals(decodeMap.size(), 5);
  }

  @Test
  public void testNestListEncoder() {
    Map<String, List<List<RowEncoderTest.Bar>>> bars = new HashMap<>();
    for (int i = 0; i < 5; i++) {
      List<List<RowEncoderTest.Bar>> bars1 = new ArrayList<>();
      for (int j = 0; j < i; j++) {
        List<RowEncoderTest.Bar> bars2 = new ArrayList<>();
        for (int k = 0; k < 3; k++) {
          RowEncoderTest.Bar bar = new RowEncoderTest.Bar();
          bar.f1 = k;
          bar.f2 = "s" + k;
          bars2.add(bar);
        }
        bars1.add(bars2);
      }
      bars.put("" + i, bars1);
    }

    MapEncoder<Map<String, List<List<RowEncoderTest.Bar>>>> encoder =
        Encoders.mapEncoder(new TypeRef<Map<String, List<List<RowEncoderTest.Bar>>>>() {});
    BinaryMap array = encoder.toMap(bars);
    Map<String, List<List<RowEncoderTest.Bar>>> newBars = encoder.fromMap(array);

    Assert.assertEquals(bars, newBars);

    byte[] bytes = encoder.encode(bars);
    Map<String, List<List<RowEncoderTest.Bar>>> decodeMap = encoder.decode(bytes);
    Assert.assertEquals(decodeMap.size(), 5);
  }

  @Test
  public void testNestArrayWithMapEncoder() {
    Map<String, List<Map<RowEncoderTest.Foo, List<RowEncoderTest.Bar>>>> lmap = new HashMap<>();
    for (int i = 0; i < 10; i++) {
      List<Map<RowEncoderTest.Foo, List<RowEncoderTest.Bar>>> list = new ArrayList<>();
      for (int j = 0; j < 3; j++) {
        Map<RowEncoderTest.Foo, List<RowEncoderTest.Bar>> map = new HashMap<>();
        RowEncoderTest.Bar bar = new RowEncoderTest.Bar();
        bar.f1 = j;
        bar.f2 = "x" + j;
        map.put(new RowEncoderTest.Foo(), Arrays.asList(bar));
        list.add(map);
      }
      lmap.put("" + i, list);
    }

    MapEncoder<Map<String, List<Map<RowEncoderTest.Foo, List<RowEncoderTest.Bar>>>>> encoder =
        Encoders.mapEncoder(
            new TypeRef<Map<String, List<Map<RowEncoderTest.Foo, List<RowEncoderTest.Bar>>>>>() {});
    BinaryMap array = encoder.toMap(lmap);
    Map<String, List<Map<RowEncoderTest.Foo, List<RowEncoderTest.Bar>>>> newLmap =
        encoder.fromMap(array);

    Assert.assertEquals(lmap, newLmap);

    byte[] bytes = encoder.encode(lmap);
    Map<String, List<Map<RowEncoderTest.Foo, List<RowEncoderTest.Bar>>>> decodeMap =
        encoder.decode(bytes);
    Assert.assertEquals(decodeMap.size(), 10);
  }
}
