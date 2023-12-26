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

package org.apache.fury.serializer;

import static org.testng.Assert.assertEquals;

import lombok.Data;
import org.apache.fury.Fury;
import org.apache.fury.config.Language;
import org.apache.fury.memory.MemoryBuffer;
import org.apache.fury.memory.MemoryUtils;
import org.apache.fury.test.bean.Cyclic;
import org.apache.fury.util.Preconditions;
import org.testng.annotations.Test;

@SuppressWarnings("unchecked")
public class ObjectSerializerTest {

  @Test
  public void testLocalClass() {
    String str = "str";
    class Foo {
      public String foo(String s) {
        return str + s;
      }
    }
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(false)
            .requireClassRegistration(false)
            .build();
    ObjectSerializer serializer = new ObjectSerializer(fury, Foo.class);
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    Foo foo = new Foo();
    serializer.write(buffer, foo);
    Object obj = serializer.read(buffer);
    assertEquals(foo.foo("str"), ((Foo) obj).foo("str"));
  }

  @Test
  public void testAnonymousClass() {
    String str = "str";
    class Foo {
      public String foo(String s) {
        return str + s;
      }
    }
    Foo foo =
        new Foo() {
          @Override
          public String foo(String s) {
            return "Anonymous " + s;
          }
        };
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(false)
            .requireClassRegistration(false)
            .build();
    ObjectSerializer serializer = new ObjectSerializer(fury, foo.getClass());
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    serializer.write(buffer, foo);
    Object obj = serializer.read(buffer);
    assertEquals(foo.foo("str"), ((Foo) obj).foo("str"));
  }

  @Test
  public void testSerializeCircularReference() {
    Cyclic cyclic = Cyclic.create(true);
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(true)
            .requireClassRegistration(false)
            .build();
    MemoryBuffer buffer = MemoryUtils.buffer(32);

    ObjectSerializer<Cyclic> serializer = new ObjectSerializer<>(fury, Cyclic.class);
    fury.getRefResolver().writeRefOrNull(buffer, cyclic);
    serializer.write(buffer, cyclic);
    byte tag = fury.getRefResolver().readRefOrNull(buffer);
    Preconditions.checkArgument(tag == Fury.REF_VALUE_FLAG);
    fury.getRefResolver().preserveRefId();
    Cyclic cyclic1 = serializer.read(buffer);
    fury.reset();
    assertEquals(cyclic1, cyclic);
  }

  @Data
  public static class A {
    Integer f1;
    Integer f2;
    Long f3;
    int f4;
    int f5;
    Integer f6;
    Long f7;
  }

  @Test
  public void testSerialization() {
    Fury fury =
        Fury.builder()
            .withLanguage(Language.JAVA)
            .withRefTracking(false)
            .requireClassRegistration(false)
            .build();
    MemoryBuffer buffer = MemoryUtils.buffer(32);
    ObjectSerializer<A> serializer = new ObjectSerializer<>(fury, A.class);
    A a = new A();
    serializer.write(buffer, a);
    assertEquals(a, serializer.read(buffer));
  }
}
