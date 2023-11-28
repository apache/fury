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

package io.fury.graalvm;

import io.fury.Fury;
import io.fury.memory.MemoryBuffer;

import java.io.FilePermission;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class Hello {
  static Fury fury;

  static {
    fury = Fury.builder().requireClassRegistration(true).build();
    fury.register(Foo.class);
    // Generate serializer code.
    fury.getClassResolver().getSerializer(Foo.class);
  }

  public static void main(String[] args) {
    System.out.println("main================");
    Foo foo = new Foo(10, "abc", List.of("str1", "str2"), Map.of("k1", 10L, "k2", 20L));
    byte[] bytes = fury.serialize(foo);
    System.out.println(fury.getClassResolver().getSerializer(Foo.class));
    Object o = fury.deserialize(bytes);
    System.out.println(foo);
    System.out.println(o);
    System.out.println(foo.equals(o));
  }
}
