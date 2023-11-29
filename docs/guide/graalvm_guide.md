<!-- fury_frontmatter --
title: GraalVM Guide
order: 6
-- fury_frontmatter -->

# GraalVM Native Image
GraalVM native image can compile java code into native code ahead to build faster, smaller, leaner applications.
Although GraalVM native image doesn't have a JIT compiler to compile bytecode into machine code, and doesn't support 
reflection unless configure reflection file.

Fury run on GraalVM native image pretty well. Fury will generate all serializer code for fury `JIT framework` and `MethodHandle/LambdaMetafactory` at graalvm build time. Then use those generated code for serialization at runtime without 
any extra cost, the performance is pretty good.

In order to use Fury on graalvm native image, you must create Fury as an **static** field of a class, and **register** all classes at
 the enclosing class initialize time. Then configure `native-image.properties` under 
`resources/META-INF/native-image/$xxx/native-image.propertie` to tell graalvm to init the class at native image 
build time. For example, here we configure `io.fury.graalvm.Example` class be init at build time:
```properties
Args = --initialize-at-build-time=io.fury.graalvm.Example
```

Another benefit using fury is that you don't have to configure [reflection json](https://www.graalvm.org/latest/reference-manual/native-image/metadata/#specifying-reflection-metadata-in-json) and 
[serialization json](https://www.graalvm.org/latest/reference-manual/native-image/metadata/#serialization), which is
very tedious, cumbersome and inconvenient. When using fury, you just need to invoke 
`io.fury.Fury.register(Class<?>, boolean)` for every type you want to serialize.

## Not thread safe fury
Example:
```java
import io.fury.Fury;
import io.fury.util.Preconditions;

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
Then add `io.fury.graalvm.Example` build time init to `native-image.properties` configuration:
```properties
Args = --initialize-at-build-time=io.fury.graalvm.Example
```

## Thread safe fury
```java
import io.fury.Fury;
import io.fury.ThreadLocalFury;
import io.fury.ThreadSafeFury;
import io.fury.util.Preconditions;

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
Then add `io.fury.graalvm.ThreadSafeExample` build time init to `native-image.properties` configuration:
```properties
Args = --initialize-at-build-time=io.fury.graalvm.ThreadSafeExample
```

## Framework Integration
For framework developers, if you want to integrate fury for serialization, you can provided a configuration file to let 
the users to list all the classes they want to serialize, then you can load those classes and invoke 
`io.fury.Fury.register(Class<?>, boolean)` to register those classes in your Fury integration class, and configure that 
class be initialized at graalvm native image build time.
