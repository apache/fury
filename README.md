<div align="center">
  <img width="77%" alt="" src="docs/images/logo/fury-logo.svg"><br>
</div>

# Fury: Blazing Fast Binary Serialization

Fury is a blazing fast multi-language serialization framework powered by jit(just-in-time compilation) and zero-copy.

## Features
- Multiple languages: Java/Python/C++/Golang/Javascript.
- Zero-copy: cross-language out-of-band serialization inspired
  by [pickle5](https://peps.python.org/pep-0574/) and off-heap read/write.
- High performance: A highly-extensible JIT framework to generate serializer code at runtime in an async multi-thread way to speed serialization, providing 20-200x speed up by:
    - reduce memory access by inline variable in generated code.
    - reduce virtual method invocation by inline call in generated code.
    - reduce conditional branching.
    - reduce hash lookup.
- Multiple binary protocols: object graph, row format and so on.

In addition to cross-language serialization, Fury also features at:

- Drop-in replace Java serialization frameworks such as JDK/Kryo/Hessian without modifying any code, but 100x faster. 
  It can greatly improve the efficiency of high-performance RPC calls, data transfer and object persistence.
- JDK serialization 100% compatible, support java custom serialization 
  `writeObject/readObject/writeReplace/readResolve/readObjectNoData` natively.
- Supports shared and circular reference object serialization for golang.
- Supports automatic object serialization for golang.

https://furyio.org

## Protocols
Different scenarios have different serialization requirements. Fury designed and implemented 
multiple binary protocols for those requirements:
- Cross-language object graph protocol:
  - Cross-language serialize any object automatically, no need for IDL definition, schema compilation and object protocol
    conversion.
  - Support shared reference and circular reference, no duplicate data or recursion error.
  - Support polymorphism.
- Native java/python object graph protocol: highly-optimized based on type system of the language.
- Row format protocol: a cache-friendly binary random access format, supports skipping serialization and partial serialization,
  and can convert to column-format automatically.

New protocols can be added easily based on fury existing buffer, encoding, codegen and other capabilities. All of those share same codebase, and the optimization for one protocol
can be reused by another protocol.

## Benchmarks
Different serialization frameworks are suitable for different scenarios, and benchmark results here are for reference only.

If you need to benchmark for your specific scenario, make sure all serialization frameworks are appropriately configured for that scenario.

Dynamic serialization frameworks need to support polymorphism and reference, which has more cost compared 
to static serialization frameworks, unless it uses the jit techniques as fury did.
Since fury will generate code at runtime, please warm up before collecting benchmark statistics.

### Java Serialization
<img width="22%" alt="" src="docs/benchmarks/serialization/bench_serialize_compatible_STRUCT_to_directBuffer_time.png">
<img width="22%" alt="" src="docs/benchmarks/serialization/bench_serialize_compatible_MEDIA_CONTENT_to_array_time.png">
<img width="22%" alt="" src="docs/benchmarks/serialization/bench_serialize_MEDIA_CONTENT_to_array_time.png">
<img width="22%" alt="" src="docs/benchmarks/serialization/bench_serialize_SAMPLE_to_array_time.png">

<img width="22%" alt="" src="docs/benchmarks/deserialization/bench_deserialize_compatible_STRUCT_from_directBuffer_time.png">
<img width="22%" alt="" src="docs/benchmarks/deserialization/bench_deserialize_compatible_MEDIA_CONTENT_from_array_time.png">
<img width="22%" alt="" src="docs/benchmarks/deserialization/bench_deserialize_MEDIA_CONTENT_from_array_time.png">
<img width="22%" alt="" src="docs/benchmarks/deserialization/bench_deserialize_SAMPLE_from_array_time.png">

See [benchmarks](https://github.com/alipay/fury/tree/main/docs/benchmarks) for more benchmarks about type forward/backward compatibility, off-heap support, zero-copy serialization.

## Installation
### Java
Nightly snapshot:
```xml
<repositories>
  <repository>
    <id>sonatype</id>
    <url>https://oss.sonatype.org/content/repositories/snapshots/</url>
    <releases>
      <enabled>false</enabled>
    </releases>
    <snapshots>
      <enabled>true</enabled>
    </snapshots>
  </repository>
</repositories>
<dependency>
  <groupId>org.furyio</groupId>
  <artifactId>fury-core</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency>
<!-- row/arrow format support -->
<!-- <dependency>
  <groupId>org.furyio</groupId>
  <artifactId>fury-format</artifactId>
  <version>0.1.0-SNAPSHOT</version>
</dependency> -->
```
Release version: coming soon.

### Python
```bash
# Python whl will be released soon. 
# Currently you need to install from the source.
git clone https://github.com/alipay/fury.git
cd fury/python
pip install -v -e .
```
### JavaScript
```bash
npm install @furyjs/fury
```
### Golang
Coming soon.

## Quickstart
Here we give a quick start about how to use fury, see [User Guide](https://github.com/alipay/fury/blob/main/docs/user_guide.md) for more details about java serialization, zero-copy and row format.

### Fury java object graph serialization
If you don't have cross-language requirements, using `Fury java object graph serialization` will 
have better performance.
```java
import io.fury.Fury;
import java.util.List;
import java.util.Arrays;

public class Example {
  public static void main(String[] args) {
    SomeClass object = new SomeClass();
    // Note that Fury instances should be reused between 
    // multiple serializations of different objects.
    {
      Fury fury = Fury.builder().withLanguage(Fury.Language.JAVA)
        .withRefTracking(true)
        // Allow to deserialize objects unknown types,
        // more flexible but less secure.
        // .withSecureMode(false)
        .build();
      // Registering types can reduce class name serialization overhead, but not mandatory.
      // If secure mode enabled, all custom types must be registered.
      fury.register(SomeClass.class);
      byte[] bytes = fury.serialize(object);
      System.out.println(fury.deserialize(bytes));
    }
    {
      ThreadSafeFury fury = Fury.builder().withLanguage(Fury.Language.JAVA)
        // Allow to deserialize objects unknown types,
        // more flexible but less secure.
        // .withSecureMode(false)
        .withRefTracking(true)
        .buildThreadSafeFury();
      byte[] bytes = fury.serialize(object);
      System.out.println(fury.deserialize(bytes));
    }
    {
      ThreadSafeFury fury = new ThreadSafeFury(() -> {
        Fury fury = Fury.builder().withLanguage(Fury.Language.JAVA)
          .withClassRegistrationRequired(false)
          .withRefTracking(true).build();
        fury.register(SomeClass.class);
        return fury;
      });
      byte[] bytes = fury.serialize(object);
      System.out.println(fury.deserialize(bytes));
    }
  }
}
```

### Cross-language object graph serialization
**Java**
```java
import com.google.common.collect.ImmutableMap;
import io.fury.*;

import java.util.Map;

public class ReferenceExample {
  public static class SomeClass {
    SomeClass f1;
    Map<String, String> f2;
    Map<String, String> f3;
  }

  public static Object createObject() {
    SomeClass obj = new SomeClass();
    obj.f1 = obj;
    obj.f2 = ImmutableMap.of("k1", "v1", "k2", "v2");
    obj.f3 = obj.f2;
    return obj;
  }

  // mvn exec:java -Dexec.mainClass="io.fury.examples.ReferenceExample"
  public static void main(String[] args) {
    Fury fury = Fury.builder().withLanguage(Language.XLANG)
      .withRefTracking(true).build();
    fury.register(SomeClass.class, "example.SomeClass");
    byte[] bytes = fury.serialize(createObject());
    // bytes can be data serialized by other languages.
    System.out.println(fury.deserialize(bytes));
    ;
  }
}
```

**Python**

```python
from typing import Dict
import pyfury

class SomeClass:
    f1: "SomeClass"
    f2: Dict[str, str]
    f3: Dict[str, str]

fury = pyfury.Fury(ref_tracking=True)
fury.register_class(SomeClass, "example.SomeClass")
obj = SomeClass()
obj.f2 = {"k1": "v1", "k2": "v2"}
obj.f1, obj.f3 = obj, obj.f2
data = fury.serialize(obj)
# bytes can be data serialized by other languages.
print(fury.deserialize(data))
```

**Golang** 

```go
package main

import furygo "github.com/alipay/fury/fury/go/fury"
import "fmt"

func main() {
	type SomeClass struct {
		F1 *SomeClass
		F2 map[string]string
		F3 map[string]string
	}
	fury := furygo.NewFury(true)
	if err := fury.RegisterTagType("example.SomeClass", SomeClass{}); err != nil {
		panic(err)
	}
	value := &SomeClass{F2: map[string]string{"k1": "v1", "k2": "v2"}}
	value.F3 = value.F2
	value.F1 = value
	bytes, err := fury.Marshal(value)
	if err != nil {
	}
	var newValue interface{}
	// bytes can be data serialized by other languages.
	if err := fury.Unmarshal(bytes, &newValue); err != nil {
		panic(err)
	}
	fmt.Println(newValue)
}
```

### Row format
#### Java
```java
public class Bar {
  String f1;
  List<Long> f2;
}

public class Foo {
  int f1;
  List<Integer> f2;
  Map<String, Integer> f3;
  List<Bar> f4;
}

Encoder<Foo> encoder = Encoders.bean(Foo.class);
Foo foo = new Foo();
foo.f1 = 10;
foo.f2 = IntStream.range(0, 1000000).boxed().collect(Collectors.toList());
foo.f3 = IntStream.range(0, 1000000).boxed().collect(Collectors.toMap(i -> "k"+i, i->i));
List<Bar> bars = new ArrayList<>(1000000);
for (int i = 0; i < 1000000; i++) {
  Bar bar = new Bar();
  bar.f1 = "s"+i;
  bar.f2 = LongStream.range(0, 10).boxed().collect(Collectors.toList());
  bars.add(bar);
}
foo.f4 = bars;
// Can be zero-copy read by python
BinaryRow binaryRow = encoder.toRow(foo);
// can be data from python
Foo newFoo = encoder.fromRow(binaryRow);
// zero-copy read List<Integer> f2
BinaryArray binaryArray2 = binaryRow.getArray(1);
// zero-copy read List<Bar> f4
BinaryArray binaryArray4 = binaryRow.getArray(4);
// zero-copy read 11th element of `readList<Bar> f4`
BinaryRow barStruct = binaryArray4.getStruct(10);

// zero-copy read 6th of f2 of 11th element of `readList<Bar> f4`
barStruct.getArray(1).getLong(5);
Encoder<Bar> barEncoder = Encoders.bean(Bar.class);
// deserialize part of data.
Bar newBar = barEncoder.fromRow(barStruct);
Bar newBar2 = barEncoder.fromRow(binaryArray4.getStruct(20));
```
#### Python
```python
@dataclass
class Bar:
    f1: str
    f2: List[pa.int64]
@dataclass
class Foo:
    f1: pa.int32
    f2: List[pa.int32]
    f3: Dict[str, pa.int32]
    f4: List[Bar]

encoder = pyfury.encoder(Foo)
foo = Foo(f1=10, f2=list(range(1000_000)),
         f3={f"k{i}": i for i in range(1000_000)},
         f4=[Bar(f1=f"s{i}", f2=list(range(10))) for i in range(1000_000)])
binary: bytes = encoder.to_row(foo).to_bytes()
foo_row = pyfury.RowData(encoder.schema, binary)
print(foo_row.f2[100000], foo_row.f4[100000].f1, foo_row.f4[200000].f2[5])
```

## Compatibility
### Schema Compatibility
Fury java object graph serialization support class schema forward/backward compatibility. The serialization peer adn deserialization peer can have different add/delete fields independently.

We plan to add support cross-language serialization after [meta compression](https://github.com/alipay/fury/issues/203) are finished.
### Binary Compatibility
We are still improving our protocols, binary compatibility are not ensured between fury releases for now. Please shade fury if you use it for now.

Binary compatibility will be ensured before fury 1.0.

## Security
Static serialization such as row format are secure in nature.
But dynamic object graph serialization supports  deserialize unregistered types, which can introduce security issues. Fury provides a secure mode option for it, and enabled by default, which can only deserialize registered types or built-in types, thus secure, by sacrificing some dynamics.

If you can ensure all environment are secure, you can disable the secure mode, thus the user types are not needed be registered ahead, and can be serialized automatically.

## RoadMap
- Meta compression, auto meta sharing and cross-language schema compatibility.
- AOT Framework for c++/golang/rust to generate code statically.
- C++/Rust object graph serialization support
- Golang/Rust/NodeJS row format support
- ProtoBuffer compatibility support
- Protocols for features and knowledge graph serialization
- Continuously improve our serialization infrastructure for any new protocols

## How to Contribute
Please read our [project development guide](https://github.com/alipay/fury/blob/main/docs/development.md).

## Getting involved

| Platform                                                                                                                                                          | Purpose                                                                                                                                                                                                   | Estimated Response Time |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|
| [GitHub Issues](https://github.com/alipay/fury/issues)                                                                                                            | For reporting bugs and filing feature requests.                                                                                                                                                           | < 1 days                |
| [Slack](https://join.slack.com/t/fury-project/shared_invite/zt-1u8soj4qc-ieYEu7ciHOqA2mo47llS8A)                                                                  | For collaborating with other Fury users and latest announcements about Fury.                                                                                                                              | < 2 days                |
| [StackOverflow](https://stackoverflow.com/questions/tagged/fury)                                                                                                  | For asking questions about how to use Fury.                                                                                                                                                               | < 2 days                |
| [Zhihu](https://www.zhihu.com/column/c_1638859452651765760)  [Twitter](https://twitter.com/fury_community)  [Youtube](https://www.youtube.com/@FurySerialization) | Follow us for latest announcements about Fury.                                                                                                                                                            | < 2 days                |
| WeChat Official Account(微信公众号) / Dingding Group(钉钉群)                                                                                                              | <div style="text-align:center;"><img src="docs/images/fury_wechat_12.jpg" alt="WeChat Official Account " width="20%"/> <img src="docs/images/fury_dingtalk.png" alt="Dingding Group" width="20%"/> </div> | < 2 days                |
