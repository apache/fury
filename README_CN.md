<div align="center">
  <img width="65%" alt="Fury Logo" src="docs/images/logo/fury_github_banner.png"><br>
</div>

[![Build Status](https://img.shields.io/github/actions/workflow/status/alipay/fury/ci.yml?branch=main&style=for-the-badge&label=GITHUB%20ACTIONS&logo=github)](https://github.com/alipay/fury)
[![Slack](https://img.shields.io/badge/join_Slack-781FF5.svg?logo=slack&style=for-the-badge)](https://join.slack.com/t/fury-project/shared_invite/zt-1u8soj4qc-ieYEu7ciHOqA2mo47llS8A)
[![Twitter](https://img.shields.io/twitter/follow/fury_community?logo=twitter&style=for-the-badge)](https://twitter.com/fury_community)

Fury是一个基于**JIT动态编译**和**零拷贝**的高性能多语言序列化框架，提供最高170x的性能和极致的易用性。

https://furyio.org

## 特性

- **多语言**: Java/Python/C++/Golang/Javascript.
- **零拷贝**: 类似[pickle5](https://peps.python.org/pep-0574/)零拷贝的跨语言零拷贝和堆外内存读写.
- **高性能**: 高度可扩展的JIT框架，可以在运行时以异步多线程的方式动态生成序列环境代码，提供20-170x的加速:
  - 通过内联变量减少内存访问
  - 通过在生成代码内联方法调用减少虚方法开销
  - 减少条件分支
  - 检查hash查找
- **多个二进制协议**: 对象图、行存等.

除了跨语言序列化，Fury也具备以下能力:

- 无缝替代Java序列化框架JDK/Kryo/Hessian等，无需修改任何用户代码，提供最高百倍以上性能，大幅改进高性能RPC调用、大规模数据传输和对象持久化效率。
- **100%兼容**JDK序列化, 原生支持JDK自定义序列化方法
  `writeObject/readObject/writeReplace/readResolve/readObjectNoData`。
- 支持golang共享引用、循环引用、指针序列化。
- 支持自动化的golang struct序列化。

## 协议

不同的协议有不同的序列化需求，Fury针对这些需求设计和实现了多个二进制协议：

- **跨语言对象图序列化协议**:
  - 跨语言自动序列化任意对象，不需要定义IDL，静态生成代码，以及在对象和生成代码之间进行转换。
  - 支持共享引用和循环引用，不会出现重复序列化和递归错误。
  - 支持对象多态。
- **Native java/python 序列化协议**: 基于语言的类型类型深度优化序列化性能和大小.
- **行存协议**: 缓存友好的二进制随机读写协议，支持跳过序列化和部分序列化，可以和Apache Arrow列存自动互转。

同时也可以基于Fury已有的buffer/encoding/meta/codegen等能力快速构建新的协议，所有协议共享同一套基础能力，针对一个协议的优化，可以让所有协议受益。

## 基准测试

不同的序列化框架适合不同场景，基准测试结果仅做参考。如果你需要针对你的场景进行性能对比，确保所有序列化框架都针对该场景进行了恰当的配置。

动态序列化框架支持多态和引用，通常情况下比静态序列化框架有更多的开销，除非跟Fury一样通过JIT技术进行了加速。由于Fury会在运行时生成代码，
**在收集
基准测试数据前请先进行预热，保证代码生成已经执行完成**

### Java序列化

标题包含"compatible"的图表支持类型前后兼容。

标题不包含"compatible"的图表表示类型需要强一致：序列化和反序列化端的class的Schema必须保持一致。

`Struct`
是一个有 [100 基本类型的字段的类](https://github.com/alipay/fury/tree/main/docs/benchmarks#Struct), `MediaContent`
是来自 [jvm-serializers](https://github.com/eishay/jvm-serializers/blob/master/tpc/src/data/media/MediaContent.java)
的类, `Sample`
是来自 [kryo benchmark](https://github.com/EsotericSoftware/kryo/blob/master/benchmarks/src/main/java/com/esotericsoftware/kryo/benchmarks/data/Sample.java)
的类.

<p align="center">
<img width="24%" alt="" src="docs/benchmarks/compatible/bench_serialize_compatible_STRUCT_to_directBuffer_tps.png">
<img width="24%" alt="" src="docs/benchmarks/compatible/bench_serialize_compatible_MEDIA_CONTENT_to_array_tps.png">
<img width="24%" alt="" src="docs/benchmarks/serialization/bench_serialize_MEDIA_CONTENT_to_array_tps.png">
<img width="24%" alt="" src="docs/benchmarks/serialization/bench_serialize_SAMPLE_to_array_tps.png">
</p>

<p align="center">
<img width="24%" alt="" src="docs/benchmarks/compatible/bench_deserialize_compatible_STRUCT_from_directBuffer_tps.png">
<img width="24%" alt="" src="docs/benchmarks/compatible/bench_deserialize_compatible_MEDIA_CONTENT_from_array_tps.png">
<img width="24%" alt="" src="docs/benchmarks/deserialization/bench_deserialize_MEDIA_CONTENT_from_array_tps.png">
<img width="24%" alt="" src="docs/benchmarks/deserialization/bench_deserialize_SAMPLE_from_array_tps.png">
</p>

可以访问 [benchmarks](https://github.com/alipay/fury/tree/main/docs/benchmarks) 查看基准测试环境、代码以及零拷贝和堆外序列化等场景测试结果。

## 安装

### Java

Nightly快照版本:

```xml

<repositories>
  <repository>
    <id>sonatype</id>
    <url>https://s01.oss.sonatype.org/content/repositories/snapshots</url>
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

正式版本:

```xml

<dependency>
  <groupId>org.furyio</groupId>
  <artifactId>fury-core</artifactId>
  <version>0.1.0-alpha.2</version>
</dependency>
  <!-- row/arrow format support -->
  <!-- <dependency>
    <groupId>org.furyio</groupId>
    <artifactId>fury-format</artifactId>
    <version>0.1.0-alpha.2</version>
  </dependency> -->
```

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

即将发布

## Quickstart

下面是一个如何使用Fury的快速指南，更多信息请查看 [用户指南](https://github.com/alipay/fury/blob/main/docs/README.md)，[跨语言序列化指南](https://github.com/alipay/fury/blob/main/docs/guide/xlang_object_graph_guide.md)， [行存指南](https://github.com/alipay/fury/blob/main/docs/guide/row_format_guide.md).

### Fury Java 序列化

该模式比跨语言序列化有更好的性能，更小的序列化大小。

```java
import io.fury.*;

import java.util.*;

public class Example {
  public static void main(String[] args) {
    SomeClass object = new SomeClass();
    // 注意应该在多次序列化之间复用Fury实例
    {
      Fury fury = Fury.builder().withLanguage(Language.JAVA)
        // 允许反序列化未知类型，不安全，但更灵活,
        // .withSecureMode(false)
        .build();
      // 注册类型可以减少类名称序列化，但不是必须的。
      // 如果安全模式开启(默认开启)，所有自定义类型必须注册。
      fury.register(SomeClass.class);
      byte[] bytes = fury.serialize(object);
      System.out.println(fury.deserialize(bytes));
    }
    {
      ThreadSafeFury fury = Fury.builder().withLanguage(Language.JAVA)
        // 允许反序列化未知类型，不安全，但更灵活,
        // .withSecureMode(false)
        .buildThreadSafeFury();
      byte[] bytes = fury.serialize(object);
      System.out.println(fury.deserialize(bytes));
    }
    {
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
}
```

### 跨语言对象图序列化

**Java**

```java
import io.fury.*;

import java.util.*;

public class ReferenceExample {
  public static class SomeClass {
    SomeClass f1;
    Map<String, String> f2;
    Map<String, String> f3;
  }

  public static Object createObject() {
    SomeClass obj = new SomeClass();
    obj.f1 = obj;
    obj.f2 = ofHashMap("k1", "v1", "k2", "v2");
    obj.f3 = obj.f2;
    return obj;
  }

  // mvn exec:java -Dexec.mainClass="io.fury.examples.ReferenceExample"
  public static void main(String[] args) {
    Fury fury = Fury.builder().withLanguage(Language.XLANG)
      .withRefTracking(true).build();
    fury.register(SomeClass.class, "example.SomeClass");
    byte[] bytes = fury.serialize(createObject());
    // bytes可以是其它语言序列化的数据.
    System.out.println(fury.deserialize(bytes));
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
# bytes可以是其它语言序列化的数据.
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
	// bytes可以是其它语言序列化的数据.
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
// 该数据可以被Python零拷贝直接读取
BinaryRow binaryRow = encoder.toRow(foo);
// 这里的数据可以来自python
Foo newFoo = encoder.fromRow(binaryRow);
// 零拷贝读取 List<Integer> f2
BinaryArray binaryArray2 = binaryRow.getArray(1);
// 零拷贝读取 List<Bar> f4
BinaryArray binaryArray4 = binaryRow.getArray(4);
// 零拷贝读取 `readList<Bar> f4` 的第11个元素
BinaryRow barStruct = binaryArray4.getStruct(10);
// 零拷贝读取 `readList<Bar> f4` 的第11个元素的第二个元素的第6个元素
barStruct.getArray(1).getLong(5);
Encoder<Bar> barEncoder = Encoders.bean(Bar.class);
// 反序列化部分数据
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

## 兼容性

### Schema兼容性

Fury Java序列化支持schema向前向后兼容。序列化和反序列化端可以独立增删字段。

我们计划在[元数据压缩](https://github.com/alipay/fury/issues/203)完成后实现跨语言schema前后兼容。

### 二进制兼容性

我们仍在改进我们的协议，目前不提供不同Fury版本之间的二进制兼容性，二进制兼容性将在1.0版本提供。

如果你未来可能会升级Fury，请提前做好数据和依赖的版本化管理。

## 安全

静态序列化通常比较安全，动态序列化如Java/Python序列化为了提供更多的动态和灵活性，支持反序列化未注册类型，引入了一定的安全风险。

比如，Java反序列化时可能会调用构造函数/`equals`/`hashCode`方法，如果这些方法内部包含了恶意代码，就可能造成任意代码执行等问题。

Fury提供了一个安全模式并默认开启，该模式只允许反序列化信任的提前注册的类型和内置类型，从而避免这类反序列化未知类型带来的风险。

**不要关闭安全模式或者类注册检查，除非你可以确保你的环境安全性。**

## 后续规划

- 元数据压缩、自动元数据共享、跨语言序列化前后兼容。
- 实现AOT框架，支持静态生成c++/golang序列化代码。
- C++/Rust对象图序列化支持。
- Golang/Rust/NodeJS行存支持
- 兼容ProtoBuffer IDL，支持基于ProtoBuffer IDL生成Fury序列化代码
- 协议扩展：特征序列化、知识图谱序列化
- 持续改进序列化基础能力，让所有协议受益。

## 如何贡献

Please read our [project development guide](https://github.com/alipay/fury/blob/main/docs/development.md).

## 加入Fury

| Platform                                                                                                                                                  | Purpose                                                                                                                                                                                                   | Estimated Response Time |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------|
| [GitHub Issues](https://github.com/alipay/fury/issues)                                                                                                    | 报告bug和提交需求                                                                                                                                                                                                | < 1 天                   |
| [Slack](https://join.slack.com/t/fury-project/shared_invite/zt-1u8soj4qc-ieYEu7ciHOqA2mo47llS8A)                                                          | 与其它用户交流，了解Fury最新动态                                                                                                                                                                                        | < 2 天                   |
| [StackOverflow](https://stackoverflow.com/questions/tagged/fury)                                                                                          | 提问如何使用Fury                                                                                                                                                                                                | < 2 天                   |
| [知乎](https://www.zhihu.com/column/c_1638859452651765760)  [推特](https://twitter.com/fury_community)  [Youtube](https://www.youtube.com/@FurySerialization) | 关注我们，了解Fury最新动态.                                                                                                                                                                                          | < 2 天                   |
| 微信公众号 / 钉钉群                                                                                                                                               | <div style="text-align:center;"><img src="docs/images/fury_wechat_12.jpg" alt="WeChat Official Account " width="20%"/> <img src="docs/images/fury_dingtalk.png" alt="Dingding Group" width="20%"/> </div> | < 2 天                   |
