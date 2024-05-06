/*
 * Copyright (C) 2007 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.fury.reflect;

import static org.testng.Assert.*;

import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.fury.type.TypeUtils;
import org.testng.annotations.Test;

public class TypeRefTest {
  static class MapObject extends LinkedHashMap<String, Object> {}

  @Test
  public void testGetSubtype() {
    // For issue: https://github.com/apache/incubator-fury/issues/1604
    TypeRef<? extends Map<String, Object>> typeRef =
        TypeUtils.mapOf(MapObject.class, String.class, Object.class);
    assertEquals(typeRef, TypeRef.of(MapObject.class));
    assertEquals(
        TypeUtils.mapOf(Map.class, String.class, Object.class),
        new TypeRef<Map<String, Object>>() {});
  }
}
