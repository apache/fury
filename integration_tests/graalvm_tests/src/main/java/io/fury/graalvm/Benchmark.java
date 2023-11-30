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
import io.fury.io.ClassLoaderObjectInputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputFilter;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Benchmark {
  static ObjectInputFilter filter = ObjectInputFilter.Config.createFilter("io.fury.graalvm.*");

  public static Object testJDKSerialization(Object o) {
    byte[] bytes = jdkSerialize(o);
    return jdkDeserialize(bytes);
  }

  public static byte[] jdkSerialize(Object data) {
    ByteArrayOutputStream bas = new ByteArrayOutputStream();
    jdkSerialize(bas, data);
    return bas.toByteArray();
  }

  public static void jdkSerialize(ByteArrayOutputStream bas, Object data) {
    bas.reset();
    try (ObjectOutputStream objectOutputStream = new ObjectOutputStream(bas)) {
      objectOutputStream.writeObject(data);
      objectOutputStream.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static Object jdkDeserialize(byte[] data) {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(data);
         ObjectInputStream objectInputStream =
           new ClassLoaderObjectInputStream(Thread.currentThread().getContextClassLoader(), bis)) {
      return objectInputStream.readObject();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static final boolean compressNumber;

  private static final Fury fury;
  static {
    String compress = System.getenv("COMPRESS_NUMBER");
    compressNumber = compress != null;
    fury = Fury.builder().withNumberCompressed(compressNumber).build();
    fury.register(Foo.class, true);
    fury.register(Struct.class, true);
  }

  public static void main(String[] args) {
    List<String> list = new ArrayList<>();
    for (int i = 0; i < 20; i++) {
      list.add("string" + i);
    }
    Map<String, Long> map = new HashMap<>();
    for (int i = 0; i < 20; i++) {
      map.put("key" + i, (long) i);
    }
    benchmark(new Foo(100, "abc", list, map));
    benchmark(Struct.create());
  }

  public static void benchmark(Object obj) {
    String furyRepeat = System.getenv("BENCHMARK_REPEAT");
    if (furyRepeat == null) {
      return;
    }
    int n = Integer.parseInt(furyRepeat);
    System.out.println("=========================");
    System.out.println("Benchmark repeat number: " + furyRepeat);
    System.out.println("Object type:" + obj.getClass());
    System.out.println("Compress number:" + compressNumber);
    System.out.println("Fury size:" + fury.serialize(obj).length);
    System.out.println("JDK size:" + jdkSerialize(obj).length);
    System.out.println("=========================");
    Object o = fury.deserialize(fury.serialize(obj));
    for (int i = 0; i < n; i++) {
      o = fury.deserialize(fury.serialize(obj));
      testJDKSerialization(o);
    }
    long start = System.nanoTime();
    for (int i = 0; i < n; i++) {
      o = fury.deserialize(fury.serialize(o));
    }
    long durationMills = (System.nanoTime() - start) / 1000_000;
    System.out.println("Fury serialization took mills:" + durationMills);
    System.out.println("Result:" + o.equals(obj));

    start = System.nanoTime();
    for (int i = 0; i < n; i++) {
      o = testJDKSerialization(o);
    }
    durationMills = (System.nanoTime() - start) / 1000_000;
    System.out.println("JDK serialization took mills:" + durationMills);
    System.out.println("Result:" + o.equals(obj));
  }
}
