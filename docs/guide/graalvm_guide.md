---
title: GraalVM Guide
sidebar_position: 6
id: graalvm_guide
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

## GraalVM Native Image

GraalVM `native image` can compile java code into native code ahead to build faster, smaller, leaner applications.
The native image doesn't have a JIT compiler to compile bytecode into machine code, and doesn't support
reflection unless configure reflection file.

Fury runs on GraalVM native image pretty well. Fury generates all serializer code for `Fury JIT framework` and `MethodHandle/LambdaMetafactory` at graalvm build time. Then use those generated code for serialization at runtime without
any extra cost, the performance is great.

In order to use Fury on graalvm native image, you must create Fury as an **static** field of a class, and **register** all classes at
 the enclosing class initialize time. Then configure `native-image.properties` under
`resources/META-INF/native-image/$xxx/native-image.propertie` to tell graalvm to init the class at native image
build time. For example, here we configure `org.apache.fury.graalvm.Example` class be init at build time:

```properties
Args = --initialize-at-build-time=org.apache.fury.graalvm.Example
```

Another benefit using fury is that you don't have to configure [reflection json](https://www.graalvm.org/latest/reference-manual/native-image/metadata/#specifying-reflection-metadata-in-json) and
[serialization json](https://www.graalvm.org/latest/reference-manual/native-image/metadata/#serialization), which is
very tedious, cumbersome and inconvenient. When using fury, you just need to invoke
`org.apache.fury.Fury.register(Class<?>, boolean)` for every type you want to serialize.

Note that Fury `asyncCompilationEnabled` option will be disabled automatically for graalvm native image since graalvm
native image doesn't support JIT at the image run time.

## Not thread-safe Fury

Example:

```java
import org.apache.fury.Fury;
import org.apache.fury.util.Preconditions;

import java.util.List;
import java.util.Map;

public class Example {
  public record Record (
    int f1,
    String f2,
    List<String> f3,
    Map<String, Long> f4) {
  }

  static Fury fury;

  static {
    fury = Fury.builder().build();
    // register and generate serializer code.
    fury.register(Record.class, true);
  }

  public static void main(String[] args) {
    Record record = new Record(10, "abc", List.of("str1", "str2"), Map.of("k1", 10L, "k2", 20L));
    System.out.println(record);
    byte[] bytes = fury.serialize(record);
    Object o = fury.deserialize(bytes);
    System.out.println(o);
    Preconditions.checkArgument(record.equals(o));
  }
}
```

Then add `org.apache.fury.graalvm.Example` build time init to `native-image.properties` configuration:

```properties
Args = --initialize-at-build-time=org.apache.fury.graalvm.Example
```

## Thread-safe Fury

```java
import org.apache.fury.Fury;
import org.apache.fury.ThreadLocalFury;
import org.apache.fury.ThreadSafeFury;
import org.apache.fury.util.Preconditions;

import java.util.List;
import java.util.Map;

public class ThreadSafeExample {
  public record Foo (
    int f1,
    String f2,
    List<String> f3,
    Map<String, Long> f4) {
  }

  static ThreadSafeFury fury;

  static {
    fury = new ThreadLocalFury(classLoader -> {
      Fury f = Fury.builder().build();
      // register and generate serializer code.
      f.register(Foo.class, true);
      return f;
    });
  }

  public static void main(String[] args) {
    System.out.println(fury.deserialize(fury.serialize("abc")));
    System.out.println(fury.deserialize(fury.serialize(List.of(1,2,3))));
    System.out.println(fury.deserialize(fury.serialize(Map.of("k1", 1, "k2", 2))));
    Foo foo = new Foo(10, "abc", List.of("str1", "str2"), Map.of("k1", 10L, "k2", 20L));
    System.out.println(foo);
    byte[] bytes = fury.serialize(foo);
    Object o = fury.deserialize(bytes);
    System.out.println(o);
  }
}
```

Then add `org.apache.fury.graalvm.ThreadSafeExample` build time init to `native-image.properties` configuration:

```properties
Args = --initialize-at-build-time=org.apache.fury.graalvm.ThreadSafeExample
```

## Framework Integration

For framework developers, if you want to integrate fury for serialization, you can provided a configuration file to let
the users to list all the classes they want to serialize, then you can load those classes and invoke
`org.apache.fury.Fury.register(Class<?>, boolean)` to register those classes in your Fury integration class, and configure that
class be initialized at graalvm native image build time.

## Benchmark

Here we give two class benchmarks between Fury and Graalvm Serialization.

When Fury compression is disabled:

- Struct: Fury is `46x speed, 43% size` compared to JDK.
- Pojo: Fury is `12x speed, 56% size` compared to JDK.

When Fury compression is enabled:

- Struct: Fury is `24x speed, 31% size` compared to JDK.
- Pojo: Fury is `12x speed, 48% size` compared to JDK.

See [[Benchmark.java](https://github.com/apache/fury/blob/main/integration_tests/graalvm_tests/src/main/java/org/apache/fury/graalvm/Benchmark.java)] for benchmark code.

### Struct Benchmark

#### Class Fields

```java
public class Struct implements Serializable {
  public int f1;
  public long f2;
  public float f3;
  public double f4;
  public int f5;
  public long f6;
  public float f7;
  public double f8;
  public int f9;
  public long f10;
  public float f11;
  public double f12;
}
```

#### Benchmark Results

No compression:

```
Benchmark repeat number: 400000
Object type: class org.apache.fury.graalvm.Struct
Compress number: false
Fury size: 76.0
JDK size: 178.0
Fury serialization took mills: 49
JDK serialization took mills: 2254
Compare speed: Fury is 45.70x speed of JDK
Compare size: Fury is 0.43x size of JDK
```

Compress number:

```
Benchmark repeat number: 400000
Object type: class org.apache.fury.graalvm.Struct
Compress number: true
Fury size: 55.0
JDK size: 178.0
Fury serialization took mills: 130
JDK serialization took mills: 3161
Compare speed: Fury is 24.16x speed of JDK
Compare size: Fury is 0.31x size of JDK
```

### Pojo Benchmark

#### Class Fields

```java
public class Foo implements Serializable {
  int f1;
  String f2;
  List<String> f3;
  Map<String, Long> f4;
}
```

#### Benchmark Results

No compression:

```
Benchmark repeat number: 400000
Object type: class org.apache.fury.graalvm.Foo
Compress number: false
Fury size: 541.0
JDK size: 964.0
Fury serialization took mills: 1663
JDK serialization took mills: 16266
Compare speed: Fury is 12.19x speed of JDK
Compare size: Fury is 0.56x size of JDK
```

Compress number:

```
Benchmark repeat number: 400000
Object type: class org.apache.fury.graalvm.Foo
Compress number: true
Fury size: 459.0
JDK size: 964.0
Fury serialization took mills: 1289
JDK serialization took mills: 15069
Compare speed: Fury is 12.11x speed of JDK
Compare size: Fury is 0.48x size of JDK
```
