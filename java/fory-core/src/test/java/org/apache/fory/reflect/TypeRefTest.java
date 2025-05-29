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

package org.apache.fory.reflect;

import static org.testng.Assert.*;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.apache.fory.Fory;
import org.apache.fory.ForyTestBase;
import org.apache.fory.collection.Tuple2;
import org.apache.fory.type.TypeUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TypeRefTest extends ForyTestBase {
  static class MapObject extends LinkedHashMap<String, Object> {}

  @Test
  public void testGetSubtype() {
    // For issue: https://github.com/apache/fory/issues/1604
    TypeRef<? extends Map<String, Object>> typeRef =
        TypeUtils.mapOf(MapObject.class, String.class, Object.class);
    assertEquals(typeRef, TypeRef.of(MapObject.class));
    assertEquals(
        TypeUtils.mapOf(Map.class, String.class, Object.class),
        new TypeRef<Map<String, Object>>() {});
  }

  @Data
  static class MyInternalClass<T> {
    public int c = 9;
    public T t;
  }

  @EqualsAndHashCode(callSuper = true)
  static class MyInternalBaseClass extends MyInternalClass<String> {
    public int d = 19;
  }

  @Data
  static class MyClass {
    protected Map<String, MyInternalClass<?>> fields;
    private transient int r = 13;

    public MyClass() {
      fields = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
      fields.put("test", new MyInternalBaseClass());
    }
  }

  @Test
  public void testWildcardType() {
    Tuple2<TypeRef<?>, TypeRef<?>> mapKeyValueType =
        TypeUtils.getMapKeyValueType(new TypeRef<Map<String, MyInternalClass<?>>>() {});
    Assert.assertEquals(mapKeyValueType.f0.getType(), String.class);
    Assert.assertEquals(
        mapKeyValueType.f1.getRawType(), new TypeRef<MyInternalClass<?>>() {}.getRawType());
  }

  @Test(dataProvider = "enableCodegen")
  public void testWildcardTypeSerialization(boolean enableCodegen) {
    // see issue https://github.com/apache/fory/issues/1633
    Fory fory = builder().withCodegen(enableCodegen).build();
    serDeCheck(fory, new MyClass());
  }
}
