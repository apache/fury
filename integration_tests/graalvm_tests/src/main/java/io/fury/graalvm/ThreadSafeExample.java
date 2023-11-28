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
import io.fury.ThreadLocalFury;
import io.fury.ThreadSafeFury;
import io.fury.collection.Collections;
import io.fury.util.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ThreadSafeExample {
  static ThreadSafeFury fury;

  static {
    fury = new ThreadLocalFury(classLoader -> {
      Fury f = Fury.builder().requireClassRegistration(true).build();
      // register and generate serializer code.
      f.register(Foo.class, true);
      return f;
    });
    System.out.println("Init fury at build time");
  }

  public static void main(String[] args) throws Throwable {
    ThreadSafeExample threadSafeExample = new ThreadSafeExample();
    threadSafeExample.test();
    System.out.println("single thread works");
    ExecutorService service = Executors.newFixedThreadPool(10);
    System.out.println("Start to submit tasks");
    for (int i = 0; i < 1000; i++) {
      service.submit(() -> {
        try {
          threadSafeExample.test();
        } catch (Throwable t) {
          threadSafeExample.throwable = t;
        }
      });
    }
    Thread.sleep(3000);
    service.shutdown();
    service.awaitTermination(10, TimeUnit.SECONDS);
    System.out.println("tasks finished");
    if (threadSafeExample.throwable != null) {
      throw threadSafeExample.throwable;
    }
    System.out.println("tasks finished succeed");
  }

  private volatile Throwable throwable;

  private  void test() {
    Preconditions.checkArgument("abc".equals(fury.deserialize(fury.serialize("abc"))));
    Preconditions.checkArgument(List.of(1,2,3).equals(fury.deserialize(fury.serialize(List.of(1,2,3)))));
    List<String> list = Collections.ofArrayList("a", "b", "c");
    Preconditions.checkArgument(list.equals(fury.deserialize(fury.serialize(list))));
    Map<String, Integer> map = Collections.ofHashMap("k1", 1, "k2", 2);
    Preconditions.checkArgument(map.equals(fury.deserialize(fury.serialize(map))));
    map = Map.of("k1", 1, "k2", 2);
    Preconditions.checkArgument(map.equals(fury.deserialize(fury.serialize(map))));
    Foo foo = new Foo(10, "abc", List.of("str1", "str2"), Map.of("k1", 10L, "k2", 20L));
    Object o = fury.deserialize(fury.serialize(foo));
    Preconditions.checkArgument(foo.equals(o));
  }
}
