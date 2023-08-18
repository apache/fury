<!-- fury_frontmatter --
title: Java Object Graph Guide
order: 0
-- fury_frontmatter -->

# Java object graph serialization

When only java object serialization needed, this mode will have better performance compared to cross-language object
graph serialization.

## Quick Start

Note that fury creation is not cheap, the **fury instances should be reused between serializations** instead of creating
it everytime.
You should keep fury to a static global variable, or instance variable of some singleton object or limited objects.

Fury for single-thread usage:

```java
import java.util.List;
import java.util.Arrays;

import io.fury.*;

public class Example {
  public static void main(String[] args) {
    SomeClass object = new SomeClass();
    // Note that Fury instances should be reused between 
    // multiple serializations of different objects.
    Fury fury = Fury.builder().withLanguage(Language.JAVA)
      // Allow to deserialize objects unknown types, more flexible 
      // but may be insecure if the classes contains malicious code.
      // .requireClassRegistration(false)
      .build();
    // Registering types can reduce class name serialization overhead, but not mandatory.
    // If class registration enabled, all custom types must be registered.
    fury.register(SomeClass.class);
    byte[] bytes = fury.serialize(object);
    System.out.println(fury.deserialize(bytes));
  }
}
```

Fury for multiple-thread usage:

```java
import java.util.List;
import java.util.Arrays;

import io.fury.*;

public class Example {
  public static void main(String[] args) {
    SomeClass object = new SomeClass();
    // Note that Fury instances should be reused between 
    // multiple serializations of different objects.
    ThreadSafeFury fury = new ThreadLocalFury(classLoader -> {
      Fury f = Fury.builder().withLanguage(Language.JAVA)
        .withClassLoader(classLoader).build();
      f.register(SomeClass.class);
      return f;
    });
    byte[] bytes = fury.serialize(object);
    System.out.println(fury.deserialize(bytes));
  }
}
```

Fury instances reuse example:

```java
import java.util.List;
import java.util.Arrays;

import io.fury.*;

public class Example {
  // reuse fury.
  private static final ThreadSafeFury fury = Fury.builder()
    // Allow to deserialize objects unknown types, more flexible 
    // but may be insecure if the classes contains malicious code.
    .requireClassRegistration(false)
    .buildThreadSafeFury();

  public static void main(String[] args) {
    SomeClass object = new SomeClass();
    byte[] bytes = fury.serialize(object);
    System.out.println(fury.deserialize(bytes));
  }
}
```

## Advanced Usage

### Fury creation

Single thread fury:

```java
Fury fury=Fury.builder()
  .withLanguage(Language.JAVA)
  // enable referecne tracking for shared/circular reference.
  // Disable it will have better performance if no duplciate reference.
  .withRefTracking(false)
  // compress int for smaller size
  // .withIntCompressed(true)
  // compress long for smaller size
  // .withLongCompressed(true)
  .withCompatibleMode(CompatibleMode.SCHEMA_CONSISTENT)
  // enable type forward/backward compatibility
  // disable it for small size and better performance.
  // .withCompatibleMode(CompatibleMode.COMPATIBLE)
  // enable async multi-threaded compilation.
  .withAsyncCompilation(true)
  .build();
  byte[]bytes=fury.serialize(object);
  System.out.println(fury.deserialize(bytes));
```

Thread-safe fury:

```java
ThreadSafeFury fury=Fury.builder()
  .withLanguage(Language.JAVA)
  // enable referecne tracking for shared/circular reference.
  // Disable it will have better performance if no duplciate reference.
  .withRefTracking(false)
  // compress int for smaller size
  // .withIntCompressed(true)
  // compress long for smaller size
  // .withLongCompressed(true)
  .withCompatibleMode(CompatibleMode.SCHEMA_CONSISTENT)
  // enable type forward/backward compatibility
  // disable it for small size and better performance.
  // .withCompatibleMode(CompatibleMode.COMPATIBLE)
  // enable async multi-threaded compilation.
  .withAsyncCompilation(true)
  .buildThreadSafeFury();
  byte[]bytes=fury.serialize(object);
  System.out.println(fury.deserialize(bytes));
```

### Security & Class Registration

`FuryBuilder#requireClassRegistration` can be used to disable class registration, this will allow to deserialize objects
unknown types,
more flexible but **may be insecure if the classes contains malicious code**.

**Do not disable class registration unless you can ensure your environment is indeed secure**.
Malicious code in `init/equals/hashCode` can be executed when deserializing unknown/untrusted types when this option
disabled.

Class registration can not only reduce security risks, but also avoid classname serialization cost.

You can register class with API `Fury#register`.

Note that class registration order is important, serialization and deserialization peer
should have same registration order.

```java
Fury fury=xxx;
  fury.register(SomeClass.class);
  fury.register(SomeClass1.class,200);
```

### Serializer Registration

You can also register a custom serializer for a class by `Fury#registerSerializer` API.

Or implement `java.io.Externalizable` for a class.

### Zero-Copy Serialization

```java
import io.fury.*;
import io.fury.serializers.BufferObject;
import io.fury.memory.MemoryBuffer;

import java.util.*;
import java.util.stream.Collectors;

public class ZeroCopyExample {
  // Note that fury instance should be reused instead of creation every time.
  static Fury fury = Fury.builder()
    .withLanguage(Language.JAVA)
    .build();

  // mvn exec:java -Dexec.mainClass="io.ray.fury.examples.ZeroCopyExample"
  public static void main(String[] args) {
    List<Object> list = Arrays.asList("str", new byte[1000], new int[100], new double[100]);
    Collection<BufferObject> bufferObjects = new ArrayList<>();
    byte[] bytes = fury.serialize(list, e -> !bufferObjects.add(e));
    List<MemoryBuffer> buffers = bufferObjects.stream()
      .map(BufferObject::toBuffer).collect(Collectors.toList());
    System.out.println(fury.deserialize(bytes, buffers));
  }
}
```

### Meta Sharing

Fury supports share type metadata (class name, field name, final field type information, etc.) between multiple
serializations in a context (ex. TCP connection), and this information will be sent to the peer during the first
serialization in the context. Based on this metadata, the peer can rebuild the same deserializer, which avoids
transmitting metadata for subsequent serializations and reduces network traffic pressure and supports type
forward/backward compatibility automatically.

```java
// Fury.builder()
//   .withLanguage(Language.JAVA)
//   .withRefTracking(false)
//   // share meta across serialization.
//   .withMetaContextShare(true)
// Not thread-safe fury.
MetaContext context=xxx;
  fury.getSerializationContext().setMetaContext(context);
  byte[]bytes=fury.serialize(o);
// Not thread-safe fury.
  MetaContext context=xxx;
  fury.getSerializationContext().setMetaContext(context);
  fury.deserialize(bytes)

// Thread-safe fury
  fury.setClassLoader(beanA.getClass().getClassLoader());
  byte[]serialized=fury.execute(
  f->{
  f.getSerializationContext().setMetaContext(context);
  return f.serialize(beanA);
  });
// thread-safe fury
  fury.setClassLoader(beanA.getClass().getClassLoader());
  Object newObj=fury.execute(
  f->{
  f.getSerializationContext().setMetaContext(context);
  return f.deserialize(serialized);
  });
```

### Deserialize un-exited classes.

Fury support deserializing Unexisted classes, this feature can be enabled
by `FuryBuilder#deserializeUnexistedClass(true)`. When enabled, and metadata sharing enabled, Fury will store
the deserialized data of this type in a lazy subclass of Map. By using the lazy map implemented by Fury, the rebalance
cost of filling map during deserialization can be avoided, which further improves performance. If this data is sent to
another process and the class exists in this process, the data will be deserialized into the object of this type without
losing any information.

If metadata sharing is not enabled, the new class data will be skipped and a UnexistedSkipClass stub object will be
returned.

## Migration

### JDK migration

If you use jdk serialization before, and you can't upgrade your client and server at the same time, which is common for
online application. Fury provided an util method `io.fury.serializer.JavaSerializer.serializedByJDK` to check whether
the binary are generated by jdk serialization, you use following pattern to make exiting serialization protocol-aware,
then upgrade serialization to fury in an async rolling-up way:

```java
if(JavaSerializer.serializedByJDK(bytes)){
  ObjectInputStream objectInputStream=xxx;
  return objectInputStream.readObject();
  }else{
  return fury.deserialize(bytes);
  }
```