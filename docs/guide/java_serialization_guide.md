---
title: Java Serialization Guide
sidebar_position: 0
id: java_object_graph_guide
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

## Java object graph serialization

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

import org.apache.fury.*;
import org.apache.fury.config.*;

public class Example {
  public static void main(String[] args) {
    SomeClass object = new SomeClass();
    // Note that Fury instances should be reused between
    // multiple serializations of different objects.
    Fury fury = Fury.builder().withLanguage(Language.JAVA)
      .requireClassRegistration(true)
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

import org.apache.fury.*;
import org.apache.fury.config.*;

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

import org.apache.fury.*;
import org.apache.fury.config.*;

public class Example {
  // reuse fury.
  private static final ThreadSafeFury fury = new ThreadLocalFury(classLoader -> {
    Fury f = Fury.builder().withLanguage(Language.JAVA)
      .withClassLoader(classLoader).build();
    f.register(SomeClass.class);
    return f;
  });

  public static void main(String[] args) {
    SomeClass object = new SomeClass();
    byte[] bytes = fury.serialize(object);
    System.out.println(fury.deserialize(bytes));
  }
}
```

## FuryBuilder  options

| Option Name                         | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | Default Value                                                  |
|-------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------|
| `timeRefIgnored`                    | Whether to ignore reference tracking of all time types registered in `TimeSerializers` and subclasses of those types when ref tracking is enabled. If ignored, ref tracking of every time type can be enabled by invoking `Fury#registerSerializer(Class, Serializer)`. For example, `fury.registerSerializer(Date.class, new DateSerializer(fury, true))`. Note that enabling ref tracking should happen before serializer codegen of any types which contain time fields. Otherwise, those fields will still skip ref tracking. | `true`                                                         |
| `compressInt`                       | Enables or disables int compression for smaller size.                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | `true`                                                         |
| `compressLong`                      | Enables or disables long compression for smaller size.                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | `true`                                                         |
| `compressString`                    | Enables or disables string compression for smaller size.                                                                                                                                                                                                                                                                                                                                                                                                                                                                          | `false`                                                        |
| `classLoader`                       | The classloader should not be updated; Fury caches class metadata. Use `LoaderBinding` or `ThreadSafeFury` for classloader updates.                                                                                                                                                                                                                                                                                                                                                                                               | `Thread.currentThread().getContextClassLoader()`               |
| `compatibleMode`                    | Type forward/backward compatibility config. Also Related to `checkClassVersion` config. `SCHEMA_CONSISTENT`: Class schema must be consistent between serialization peer and deserialization peer. `COMPATIBLE`: Class schema can be different between serialization peer and deserialization peer. They can add/delete fields independently. [See more](#class-inconsistency-and-class-version-check).                                                                                                                            | `CompatibleMode.SCHEMA_CONSISTENT`                             |
| `checkClassVersion`                 | Determines whether to check the consistency of the class schema. If enabled, Fury checks, writes, and checks consistency using the `classVersionHash`. It will be automatically disabled when `CompatibleMode#COMPATIBLE` is enabled. Disabling is not recommended unless you can ensure the class won't evolve.                                                                                                                                                                                                                  | `false`                                                        |
| `checkJdkClassSerializable`         | Enables or disables checking of `Serializable` interface for classes under `java.*`. If a class under `java.*` is not `Serializable`, Fury will throw an `UnsupportedOperationException`.                                                                                                                                                                                                                                                                                                                                         | `true`                                                         |
| `registerGuavaTypes`                | Whether to pre-register Guava types such as `RegularImmutableMap`/`RegularImmutableList`. These types are not public API, but seem pretty stable.                                                                                                                                                                                                                                                                                                                                                                                 | `true`                                                         |
| `requireClassRegistration`          | Disabling may allow unknown classes to be deserialized, potentially causing security risks.                                                                                                                                                                                                                                                                                                                                                                                                                                       | `true`                                                         |
| `suppressClassRegistrationWarnings` | Whether to suppress class registration warnings. The warnings can be used for security audit, but may be annoying, this suppression will be enabled by default.                                                                                                                                                                                                                                                                                                                                                                   | `true`                                                         |
| `metaShareEnabled`                  | Enables or disables meta share mode.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | `true` if `CompatibleMode.Compatible` is set, otherwise false. |
| `scopedMetaShareEnabled`            | Scoped meta share focuses on a single serialization process. Metadata created or identified during this process is exclusive to it and is not shared with by other serializations.                                                                                                                                                                                                                                                                                                                                                | `true` if `CompatibleMode.Compatible` is set, otherwise false. |
| `metaCompressor`                    | Set a compressor for meta compression. Note that the passed MetaCompressor should be thread-safe. By default, a `Deflater` based compressor `DeflaterMetaCompressor` will be used. Users can pass other compressor such as `zstd` for better compression rate.                                                                                                                                                                                                                                                                    | `DeflaterMetaCompressor`                                       |
| `deserializeNonexistentClass`       | Enables or disables deserialization/skipping of data for non-existent classes.                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `true` if `CompatibleMode.Compatible` is set, otherwise false. |
| `codeGenEnabled`                    | Disabling may result in faster initial serialization but slower subsequent serializations.                                                                                                                                                                                                                                                                                                                                                                                                                                        | `true`                                                         |
| `asyncCompilationEnabled`           | If enabled, serialization uses interpreter mode first and switches to JIT serialization after async serializer JIT for a class is finished.                                                                                                                                                                                                                                                                                                                                                                                       | `false`                                                        |
| `scalaOptimizationEnabled`          | Enables or disables Scala-specific serialization optimization.                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | `false`                                                        |
| `copyRef`                           | When disabled, the copy performance will be better. But fury deep copy will ignore circular and shared reference. Same reference of an object graph will be copied into different objects in one `Fury#copy`.                                                                                                                                                                                                                                                                                                                     | `true`                                                         |
| `serializeEnumByName`               | When Enabled, fury serialize enum by name instead of ordinal.                                                                                                                                                                                                                                                                                                                                                                                                                                                                     | `false`                                                        |

## Advanced Usage

### Fury creation

Single thread fury:

```java
Fury fury = Fury.builder()
  .withLanguage(Language.JAVA)
  // enable reference tracking for shared/circular reference.
  // Disable it will have better performance if no duplicate reference.
  .withRefTracking(false)
  .withCompatibleMode(CompatibleMode.SCHEMA_CONSISTENT)
  // enable type forward/backward compatibility
  // disable it for small size and better performance.
  // .withCompatibleMode(CompatibleMode.COMPATIBLE)
  // enable async multi-threaded compilation.
  .withAsyncCompilation(true)
  .build();
byte[] bytes = fury.serialize(object);
System.out.println(fury.deserialize(bytes));
```

Thread-safe fury:

```java
ThreadSafeFury fury = Fury.builder()
  .withLanguage(Language.JAVA)
  // enable reference tracking for shared/circular reference.
  // Disable it will have better performance if no duplicate reference.
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
byte[] bytes = fury.serialize(object);
System.out.println(fury.deserialize(bytes));
```

### Handling Class Schema Evolution in Serialization

In many systems, the schema of a class used for serialization may change over time. For instance, fields within a class
may be added or removed. When serialization and deserialization processes use different versions of jars, the schema of
the class being deserialized may differ from the one used during serialization.

By default, Fury serializes objects using the `CompatibleMode.SCHEMA_CONSISTENT` mode. This mode assumes that the
deserialization process uses the same class schema as the serialization process, minimizing payload overhead.
However, if there is a schema inconsistency, deserialization will fail.

If the schema is expected to change, to make deserialization succeed, i.e. schema forward/backward compatibility.
Users must configure Fury to use `CompatibleMode.COMPATIBLE`. This can be done using the
`FuryBuilder#withCompatibleMode(CompatibleMode.COMPATIBLE)` method.
In this compatible mode, deserialization can handle schema changes such as missing or extra fields, allowing it to
succeed even when the serialization and deserialization processes have different class schemas.

Here is an example of creating Fury to support schema evolution:

```java
Fury fury = Fury.builder()
  .withCompatibleMode(CompatibleMode.COMPATIBLE)
  .build();

byte[] bytes = fury.serialize(object);
System.out.println(fury.deserialize(bytes));
```

This compatible mode involves serializing class metadata into the serialized output. Despite Fury's use of
sophisticated compression techniques to minimize overhead, there is still some additional space cost associated with
class metadata.

To further reduce metadata costs, Fury introduces a class metadata sharing mechanism, which allows the metadata to be
sent to the deserialization process only once. For more details, please refer to the [Meta Sharing](#MetaSharing)
section.

### Smaller size

`FuryBuilder#withIntCompressed`/`FuryBuilder#withLongCompressed` can be used to compress int/long for smaller size.
Normally compress int is enough.

Both compression are enabled by default, if the serialized is not important, for example, you use flatbuffers for
serialization before, which doesn't compress anything, then you should disable compression. If your data are all
numbers,
the compression may bring 80% performance regression.

For int compression, fury use 1~5 bytes for encoding. First bit in every byte indicate whether has next byte. if first
bit is set, then next byte will be read util first bit of next byte is unset.

For long compression, fury support two encoding:

- Fury SLI(Small long as int) Encoding (**used by default**):
  - If long is in `[-1073741824, 1073741823]`, encode as 4 bytes int: `| little-endian: ((int) value) << 1 |`
  - Otherwise write as 9 bytes: `| 0b1 | little-endian 8bytes long |`
- Fury PVL(Progressive Variable-length Long) Encoding:
  - First bit in every byte indicate whether has next byte. if first bit is set, then next byte will be read util
      first bit of next byte is unset.
  - Negative number will be converted to positive number by `(v << 1) ^ (v >> 63)` to reduce cost of small negative
      numbers.

If a number are `long` type, it can't be represented by smaller bytes mostly, the compression won't get good enough
result,
not worthy compared to performance cost. Maybe you should try to disable long compression if you find it didn't bring
much
space savings.

### Object deep copy

Deep copy example:

```java
Fury fury = Fury.builder().withRefCopy(true).build();
SomeClass a = xxx;
SomeClass copied = fury.copy(a);
```

Make fury deep copy ignore circular and shared reference, this deep copy mode will ignore circular and shared reference.
Same reference of an object graph will be copied into different objects in one `Fury#copy`.

```java
Fury fury = Fury.builder().withRefCopy(false).build();
SomeClass a = xxx;
SomeClass copied = fury.copy(a);
```

### Implement a customized serializer

In some cases, you may want to implement a serializer for your type, especially some class customize serialization by
JDK
writeObject/writeReplace/readObject/readResolve, which is very inefficient. For example, you don't want
following `Foo#writeObject`
got invoked, you can take following `FooSerializer` as an example:

```java
class Foo {
  public long f1;

  private void writeObject(ObjectOutputStream s) throws IOException {
    System.out.println(f1);
    s.defaultWriteObject();
  }
}

class FooSerializer extends Serializer<Foo> {
  public FooSerializer(Fury fury) {
    super(fury, Foo.class);
  }

  @Override
  public void write(MemoryBuffer buffer, Foo value) {
    buffer.writeInt64(value.f1);
  }

  @Override
  public Foo read(MemoryBuffer buffer) {
    Foo foo = new Foo();
    foo.f1 = buffer.readInt64();
    return foo;
  }
}
```

Register serializer:

```java
Fury fury = getFury();
fury.registerSerializer(Foo.class, new FooSerializer(fury));
```

### Security & Class Registration

`FuryBuilder#requireClassRegistration` can be used to disable class registration, this will allow to deserialize objects
unknown types,
more flexible but **may be insecure if the classes contains malicious code**.

**Do not disable class registration unless you can ensure your environment is secure**.
Malicious code in `init/equals/hashCode` can be executed when deserializing unknown/untrusted types when this option
disabled.

Class registration can not only reduce security risks, but also avoid classname serialization cost.

You can register class with API `Fury#register`.

Note that class registration order is important, serialization and deserialization peer
should have same registration order.

```java
Fury fury = xxx;
fury.register(SomeClass.class);
fury.register(SomeClass1.class, 200);
```

If you invoke `FuryBuilder#requireClassRegistration(false)` to disable class registration check,
you can set `org.apache.fury.resolver.ClassChecker` by `ClassResolver#setClassChecker` to control which classes are
allowed
for serialization. For example, you can allow classes started with `org.example.*` by:

```java
Fury fury = xxx;
fury.getClassResolver().setClassChecker(
  (classResolver, className) -> className.startsWith("org.example."));
```

```java
AllowListChecker checker = new AllowListChecker(AllowListChecker.CheckLevel.STRICT);
ThreadSafeFury fury = new ThreadLocalFury(classLoader -> {
  Fury f = Fury.builder().requireClassRegistration(true).withClassLoader(classLoader).build();
  f.getClassResolver().setClassChecker(checker);
  checker.addListener(f.getClassResolver());
  return f;
});
checker.allowClass("org.example.*");
```

Fury also provided a `org.apache.fury.resolver.AllowListChecker` which is allowed/disallowed list based checker to
simplify
the customization of class check mechanism. You can use this checker or implement more sophisticated checker by
yourself.

### Register class by name

Register class by id will have better performance and smaller space overhead. But in some cases, management for a bunch
of type id is complex. In such cases, registering class by name using API
`register(Class<?> cls, String namespace, String typeName)` is recommended.

```java
fury.register(Foo.class, "demo", "Foo");
```

If there are no duplicate name for type, `namespace` can be left as empty to reduce serialized size.

**Do not use this API to register class since it will increase serialized size a lot compared to register
class by id**

### Serializer Registration

You can also register a custom serializer for a class by `Fury#registerSerializer` API.

Or implement `java.io.Externalizable` for a class.

### Zero-Copy Serialization

```java
import org.apache.fury.*;
import org.apache.fury.config.*;
import org.apache.fury.serializer.BufferObject;
import org.apache.fury.memory.MemoryBuffer;

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
MetaContext context = xxx;
fury.getSerializationContext().setMetaContext(context);
byte[] bytes = fury.serialize(o);
// Not thread-safe fury.
MetaContext context = xxx;
fury.getSerializationContext().setMetaContext(context);
fury.deserialize(bytes);

// Thread-safe fury
fury.setClassLoader(beanA.getClass().getClassLoader());
byte[] serialized = fury.execute(
  f -> {
    f.getSerializationContext().setMetaContext(context);
    return f.serialize(beanA);
  }
);
// thread-safe fury
fury.setClassLoader(beanA.getClass().getClassLoader());
Object newObj = fury.execute(
  f -> {
    f.getSerializationContext().setMetaContext(context);
    return f.deserialize(serialized);
  }
);
```

### Deserialize non-existent classes

Fury support deserializing non-existent classes, this feature can be enabled
by `FuryBuilder#deserializeNonexistentClass(true)`. When enabled, and metadata sharing enabled, Fury will store
the deserialized data of this type in a lazy subclass of Map. By using the lazy map implemented by Fury, the rebalance
cost of filling map during deserialization can be avoided, which further improves performance. If this data is sent to
another process and the class exists in this process, the data will be deserialized into the object of this type without
losing any information.

If metadata sharing is not enabled, the new class data will be skipped and an `NonexistentSkipClass` stub object will be
returned.

### Coping/Mapping object from one type to another type

Fury support mapping object from one type to another type.
> Notes:
>
> 1. This mapping will execute a deep copy, all mapped fields are serialized into binary and
     deserialized from that binary to map into another type.
> 2. All struct types must be registered with same ID, otherwise Fury can not mapping to correct struct type.
     > Be careful when you use `Fury#register(Class)`, because fury will allocate an auto-grown ID which might be
     > inconsistent if you register classes with different order between Fury instance.

```java
public class StructMappingExample {
  static class Struct1 {
    int f1;
    String f2;

    public Struct1(int f1, String f2) {
      this.f1 = f1;
      this.f2 = f2;
    }
  }

  static class Struct2 {
    int f1;
    String f2;
    double f3;
  }

  static ThreadSafeFury fury1 = Fury.builder()
    .withCompatibleMode(CompatibleMode.COMPATIBLE).buildThreadSafeFury();
  static ThreadSafeFury fury2 = Fury.builder()
    .withCompatibleMode(CompatibleMode.COMPATIBLE).buildThreadSafeFury();

  static {
    fury1.register(Struct1.class);
    fury2.register(Struct2.class);
  }

  public static void main(String[] args) {
    Struct1 struct1 = new Struct1(10, "abc");
    Struct2 struct2 = (Struct2) fury2.deserialize(fury1.serialize(struct1));
    Assert.assertEquals(struct2.f1, struct1.f1);
    Assert.assertEquals(struct2.f2, struct1.f2);
    struct1 = (Struct1) fury1.deserialize(fury2.serialize(struct2));
    Assert.assertEquals(struct1.f1, struct2.f1);
    Assert.assertEquals(struct1.f2, struct2.f2);
  }
}
```

## Migration

### JDK migration

If you use jdk serialization before, and you can't upgrade your client and server at the same time, which is common for
online application. Fury provided an util method `org.apache.fury.serializer.JavaSerializer.serializedByJDK` to check
whether
the binary are generated by jdk serialization, you use following pattern to make exiting serialization protocol-aware,
then upgrade serialization to fury in an async rolling-up way:

```java
if (JavaSerializer.serializedByJDK(bytes)) {
  ObjectInputStream objectInputStream=xxx;
  return objectInputStream.readObject();
} else {
  return fury.deserialize(bytes);
}
```

### Upgrade fury

Currently binary compatibility is ensured for minor versions only. For example, if you are using fury`v0.2.0`, binary
compatibility will
be provided if you upgrade to fury `v0.2.1`. But if upgrade to fury `v0.4.1`, no binary compatibility are ensured.
Most of the time there is no need to upgrade fury to newer major version, the current version is fast and compact
enough,
and we provide some minor fix for recent older versions.

But if you do want to upgrade fury for better performance and smaller size, you need to write fury version as header to
serialized data
using code like following to keep binary compatibility:

```java
MemoryBuffer buffer = xxx;
buffer.writeVarInt32(2);
fury.serialize(buffer, obj);
```

Then for deserialization, you need:

```java
MemoryBuffer buffer = xxx;
int furyVersion = buffer.readVarInt32();
Fury fury = getFury(furyVersion);
fury.deserialize(buffer);
```

`getFury` is a method to load corresponding fury, you can shade and relocate different version of fury to different
package, and load fury by version.

If you upgrade fury by minor version, or you won't have data serialized by older fury, you can upgrade fury directly,
no need to `versioning` the data.

## Trouble shooting

### Class inconsistency and class version check

If you create fury without setting `CompatibleMode` to `org.apache.fury.config.CompatibleMode.COMPATIBLE`, and you got a
strange
serialization error, it may be caused by class inconsistency between serialization peer and deserialization peer.

In such cases, you can invoke `FuryBuilder#withClassVersionCheck` to create fury to validate it, if deserialization
throws `org.apache.fury.exception.ClassNotCompatibleException`, it shows class are inconsistent, and you should create
fury with
`FuryBuilder#withCompaibleMode(CompatibleMode.COMPATIBLE)`.

`CompatibleMode.COMPATIBLE` has more performance and space cost, do not set it by default if your classes are always
consistent between serialization and deserialization.

### Deserialize POJO into another type

Fury allows you to serialize one POJO and deserialize it into a different POJO. The different POJO means the schema inconsistency. Users must to configure Fury with
`CompatibleMode` set to `org.apache.fury.config.CompatibleMode.COMPATIBLE`.

```java
public class DeserializeIntoType {
  static class Struct1 {
    int f1;
    String f2;

    public Struct1(int f1, String f2) {
      this.f1 = f1;
      this.f2 = f2;
    }
  }

  static class Struct2 {
    int f1;
    String f2;
    double f3;
  }

  static ThreadSafeFury fury = Fury.builder()
    .withCompatibleMode(CompatibleMode.COMPATIBLE).buildThreadSafeFury();

  public static void main(String[] args) {
    Struct1 struct1 = new Struct1(10, "abc");
    byte[] data = fury.serializeJavaObject(struct1);
    Struct2 struct2 = (Struct2) fury.deserializeJavaObject(bytes, Struct2.class);
  }
}
```

### Use wrong API for deserialization

If you serialize an object by invoking `Fury#serialize`, you should invoke `Fury#deserialize` for deserialization
instead of
`Fury#deserializeJavaObject`.

If you serialize an object by invoking `Fury#serializeJavaObject`, you should invoke `Fury#deserializeJavaObject` for
deserialization instead of `Fury#deserializeJavaObjectAndClass`/`Fury#deserialize`.

If you serialize an object by invoking `Fury#serializeJavaObjectAndClass`, you should
invoke `Fury#deserializeJavaObjectAndClass` for deserialization instead
of `Fury#deserializeJavaObject`/`Fury#deserialize`.
