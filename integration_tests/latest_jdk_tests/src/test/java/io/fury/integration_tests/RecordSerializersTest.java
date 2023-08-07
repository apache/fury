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

package io.fury.integration_tests;

import static io.fury.collection.Collections.ofArrayList;
import static io.fury.collection.Maps.ofHashMap;

import io.fury.Fury;
import io.fury.serializer.CompatibleMode;
import io.fury.test.bean.Struct;
import io.fury.util.RecordComponent;
import io.fury.util.RecordUtils;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.Test;

public class RecordSerializersTest {

  public record Foo(int f1, String f2, List<String> f3, char f4) {}

  @Test
  public void testIsRecord() {
    Assert.assertTrue(RecordUtils.isRecord(Foo.class));
  }

  @Test
  public void testGetRecordComponents() {
    RecordComponent[] recordComponents = RecordUtils.getRecordComponents(Foo.class);
    Assert.assertNotNull(recordComponents);
    java.lang.reflect.RecordComponent[] expectComponents = Foo.class.getRecordComponents();
    Assert.assertEquals(recordComponents.length, expectComponents.length);
    Assert.assertEquals(
        recordComponents[0].getDeclaringRecord(), expectComponents[0].getDeclaringRecord());
    Assert.assertEquals(recordComponents[0].getType(), expectComponents[0].getType());
    Assert.assertEquals(recordComponents[0].getName(), expectComponents[0].getName());
  }

  @Test
  public void testGetRecordGenerics() {
    RecordComponent[] recordComponents = RecordUtils.getRecordComponents(Foo.class);
    Assert.assertNotNull(recordComponents);
    Type genericType = recordComponents[2].getGenericType();
    ParameterizedType parameterizedType = (ParameterizedType) genericType;
    Assert.assertEquals(parameterizedType.getActualTypeArguments()[0], String.class);
  }

  @Test
  public void testSimpleRecord() {
    Fury fury = Fury.builder().requireClassRegistration(false).withCodegen(false).build();
    Foo foo = new Foo(10, "abc", new ArrayList<>(Arrays.asList("a", "b")), 'x');
    Assert.assertEquals(fury.deserialize(fury.serialize(foo)), foo);
  }

  @Test
  public void testRecordCompatible() throws Throwable {
    String code1 =
        "import java.util.*;"
            + "public record TestRecord(int f1, String f2, List<String> f3, char f4, Map<String, Integer> f5) {}";
    Class<?> cls1 = Struct.createStructClass("TestRecord", code1);
    Object record1 =
        RecordUtils.getRecordConstructor(cls1)
            .f1
            .invoke(1, "abc", ofArrayList("a", "b"), 'a', ofHashMap("a", 1));
    Fury fury =
        Fury.builder()
            .requireClassRegistration(false)
            .withCodegen(false)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .build();
    byte[] bytes1 = fury.serialize(record1);
    Object o = fury.deserialize(bytes1);
    Assert.assertEquals(record1, o);
    String code2 =
        "import java.util.*;"
            + "public record TestRecord(int f1, String f2, char f4, Map<String, Integer> f5) {}";
    Class<?> cls2 = Struct.createStructClass("TestRecord", code2);
    Object record2 =
        RecordUtils.getRecordConstructor(cls2).f1.invoke(1, "abc", 'a', ofHashMap("a", 1));
    Fury fury2 =
        Fury.builder()
            .requireClassRegistration(false)
            .withCodegen(false)
            .withCompatibleMode(CompatibleMode.COMPATIBLE)
            .build();
    Object o2 = fury2.deserialize(fury2.serialize(record2));
    Assert.assertEquals(record2, o2);
    // test compatible
    Assert.assertEquals(fury2.deserialize(bytes1), record2);
  }
}
