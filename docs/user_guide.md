# User guide

## Cross-language object graph serialization

### Serialize built-in types
Common types can be serialized automatically: primitive numeric types, string, binary, array, list, map and so on.


#### Java

```java
import io.fury.*;

import java.util.*;

public class Example1 {
  public static void main(String[] args) {
    Fury fury = Fury.builder().withLanguage(Language.XLANG)
      .withRefTracking(false).build();
    List<Object> list = Arrays.asList(true, false, "str", -1.1, 1, new int[100], new double[20]);
    byte[] bytes = fury.serialize(list);
    // bytes can be data serialized by other languages.
    fury.deserialize(bytes);
    Map<Object, Object> map = new HashMap<>();
    map.put("k1", "v1");
    map.put("k2", list);
    map.put("k3", -1);
    bytes = fury.serialize(map);
    // bytes can be data serialized by other languages.
    fury.deserialize(bytes);
  }
}
```

#### Python

```python
import pyfury
import numpy as np

fury = pyfury.Fury(ref_tracking=False)
object_list = [True, False, "str", -1.1, 1,
               np.full(100, 0, dtype=np.int32), np.full(20, 0.0, dtype=np.double)]
data = fury.serialize(object_list)
# bytes can be data serialized by other languages.
new_list = fury.deserialize(data)
object_map = {"k1": "v1", "k2": object_list, "k3": -1}
data = fury.serialize(object_map)
# bytes can be data serialized by other languages.
new_map = fury.deserialize(data)
print(new_map)
```

#### GoLang

```go
package main

import furygo "github.com/alipay/fury/fury/go/fury"
import "fmt"

func main() {
	list := []interface{}{true, false, "str", -1.1, 1, make([]int32, 10), make([]float64, 20)}
	fury := furygo.NewFury(false)
	bytes, err := fury.Marshal(list)
	if err != nil {
		panic(err)
	}
	var newValue interface{}
	// bytes can be data serialized by other languages.
	if err := fury.Unmarshal(bytes, &newValue); err != nil {
		panic(err)
	}
	fmt.Println(newValue)
	dict := map[string]interface{}{
		"k1": "v1",
		"k2": list,
		"k3": -1,
	}
	bytes, err = fury.Marshal(dict)
	if err != nil {
		panic(err)
	}
	// bytes can be data serialized by other languages.
	if err := fury.Unmarshal(bytes, &newValue); err != nil {
		panic(err)
	}
	fmt.Println(newValue)
}
```

#### Javascript

```javascript
// Coming soon
```

### Serialize custom types
Serializing user-defined types needs registering the custom type using the register API to establish the mapping relationship between the type in different languages.
#### Java

```java
public class Example2 {
  public static class SomeClass1 {
    Object f1;
    Map<Byte, Integer> f2;
  }

  public static class SomeClass2 {
    Object f1;
    String f2;
    List<Object> f3;
    Map<Byte, Integer> f4;
    Byte f5;
    Short f6;
    Integer f7;
    Long f8;
    Float f9;
    Double f10;
    short[] f11;
    List<Short> f12;
  }

  public static Object createObject() {
    SomeClass1 obj1 = new SomeClass1();
    obj1.f1 = true;
    obj1.f2 = ImmutableMap.of((byte) -1, 2);
    SomeClass2 obj = new SomeClass2();
    obj.f1 = obj1;
    obj.f2 = "abc";
    obj.f3 = Arrays.asList("abc", "abc");
    obj.f4 = ImmutableMap.of((byte) 1, 2);
    obj.f5 = Byte.MAX_VALUE;
    obj.f6 = Short.MAX_VALUE;
    obj.f7 = Integer.MAX_VALUE;
    obj.f8 = Long.MAX_VALUE;
    obj.f9 = 1.0f / 2;
    obj.f10 = 1 / 3.0;
    obj.f11 = new short[]{(short) 1, (short) 2};
    obj.f12 = ImmutableList.of((short) -1, (short) 4);
    return obj;
  }

  // mvn exec:java -Dexec.mainClass="io.fury.examples.Example2"
  public static void main(String[] args) {
    Fury fury = Fury.builder().withLanguage(Language.XLANG).withRefTracking(false).build();
    fury.register(SomeClass1.class, "example.SomeClass1");
    fury.register(SomeClass2.class, "example.SomeClass2");
    byte[] bytes = fury.serialize(createObject());
    // bytes can be data serialized by other languages.
    System.out.println(fury.deserialize(bytes));
    ;
  }
}
```

#### Python

```python
from dataclasses import dataclass
from typing import List, Dict, Any
import pyfury, array


@dataclass
class SomeClass1:
    f1: Any
    f2: Dict[pyfury.Int8Type, pyfury.Int32Type]


@dataclass
class SomeClass2:
    f1: Any = None
    f2: str = None
    f3: List[str] = None
    f4: Dict[pyfury.Int8Type, pyfury.Int32Type] = None
    f5: pyfury.Int8Type = None
    f6: pyfury.Int16Type = None
    f7: pyfury.Int32Type = None
    # int type will be taken as `pyfury.Int64Type`.
    # use `pyfury.Int32Type` for type hint if peer
    # are more narrow type.
    f8: int = None
    f9: pyfury.Float32Type = None
    # float type will be taken as `pyfury.Float64Type`
    f10: float = None
    f11: pyfury.Int16ArrayType = None
    f12: List[pyfury.Int16Type] = None


if __name__ == "__main__":
    f = pyfury.Fury(ref_tracking=False)
    f.register_class(SomeClass1, "example.SomeClass1")
    f.register_class(SomeClass2, "example.SomeClass2")
    obj1 = SomeClass1(f1=True, f2={-1: 2})
    obj = SomeClass2(
        f1=obj1,
        f2="abc",
        f3=["abc", "abc"],
        f4={1: 2},
        f5=2 ** 7 - 1,
        f6=2 ** 15 - 1,
        f7=2 ** 31 - 1,
        f8=2 ** 63 - 1,
        f9=1.0 / 2,
        f10=1 / 3.0,
        f11=array.array("h", [1, 2]),
        f12=[-1, 4],
    )
    data = f.serialize(obj)
    # bytes can be data serialized by other languages.
    print(f.deserialize(data))
```

#### Golang

```go
package main

import furygo "github.com/alipay/fury/fury/go/fury"
import "fmt"

func main() {
	type SomeClass1 struct {
		F1  interface{}
		F2  string
		F3  []interface{}
		F4  map[int8]int32
		F5  int8
		F6  int16
		F7  int32
		F8  int64
		F9  float32
		F10 float64
		F11 []int16
		F12 fury.Int16Slice
	}

	type SomeClas2 struct {
		F1 interface{}
		F2 map[int8]int32
	}
	fury := furygo.NewFury(false)
	if err := fury.RegisterTagType("example.SomeClass1", SomeClass1{}); err != nil {
		panic(err)
	}
	if err := fury.RegisterTagType("example.SomeClass2", SomeClass2{}); err != nil {
		panic(err)
	}
	obj1 := &SomeClass1{}
	obj1.F1 = true
	obj1.F2 = map[int8]int32{-1: 2}
	obj := &SomeClass1{}
	obj.F1 = obj1
	obj.F2 = "abc"
	obj.F3 = []interface{}{"abc", "abc"}
	f4 := map[int8]int32{1: 2}
	obj.F4 = f4
	obj.F5 = fury.MaxInt8
	obj.F6 = fury.MaxInt16
	obj.F7 = fury.MaxInt32
	obj.F8 = fury.MaxInt64
	obj.F9 = 1.0 / 2
	obj.F10 = 1 / 3.0
	obj.F11 = []int16{1, 2}
	obj.F12 = []int16{-1, 4}
	bytes, err := fury.Marshal(obj);
	if err != nil {
		panic(err)
	}
	var newValue interface{}
	// bytes can be data serialized by other languages.
	if err := fury.Unmarshal(bytes, &newValue); err != nil {
		panic(err)
	}
	fmt.Println(newValue)
}
```

#### Javascript

```javascript
// Coming soon
```

### Serialize Shared Reference and Circular Reference
Shared reference and circular reference can be serialized automatically, no duplicate data or recursion error.
#### Java

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

#### Python

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

#### Golang

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

#### Javascript

```javascript
// Coming soon
```

### Zero-Copy Serialization

#### Java

```java
import io.fury.*;
import io.fury.serializers.BufferObject;
import io.fury.memory.MemoryBuffer;

import java.util.*;
import java.util.stream.Collectors;

public class ZeroCopyExample {
  // mvn exec:java -Dexec.mainClass="io.ray.fury.examples.ZeroCopyExample"
  public static void main(String[] args) {
    Fury fury = Fury.builder().withLanguage(Language.XLANG).build();
    List<Object> list = Arrays.asList("str", new byte[1000], new int[100], new double[100]);
    Collection<BufferObject> bufferObjects = new ArrayList<>();
    byte[] bytes = fury.serialize(list, e -> !bufferObjects.add(e));
    // bytes can be data serialized by other languages.
    List<MemoryBuffer> buffers = bufferObjects.stream()
      .map(BufferObject::toBuffer).collect(Collectors.toList());
    System.out.println(fury.deserialize(bytes, buffers));
  }
}
```

#### Python

```python
import array
import pyfury
import numpy as np

fury = pyfury.Fury()
list_ = ["str", bytes(bytearray(1000)),
         array.array("i", range(100)), np.full(100, 0.0, dtype=np.double)]
serialized_objects = []
data = fury.serialize(list_, buffer_callback=serialized_objects.append)
buffers = [o.to_buffer() for o in serialized_objects]
# bytes can be data serialized by other languages.
print(fury.deserialize(data, buffers=buffers))
```

#### Golang

```go
package main

import furygo "github.com/alipay/fury/fury/go/fury"
import "fmt"

func main() {
	fury := furygo.NewFury(true)
	list := []interface{}{"str", make([]byte, 1000)}
	buf := fury.NewByteBuffer(nil)
	var bufferObjects []fury.BufferObject
	fury.Serialize(buf, list, func(o fury.BufferObject) bool {
		bufferObjects = append(bufferObjects, o)
		return false
	})
	var newList []interface{}
	var buffers []*fury.ByteBuffer
	for _, o := range bufferObjects {
		buffers = append(buffers, o.ToBuffer())
	}
	if err := fury.Deserialize(buf, &newList, buffers); err != nil {
		panic(err)
	}
	fmt.Println(newList)
}
```

#### Javascript

```javascript
// Coming soon
```

## Java object graph serialization
When only java object serialization needed, this mode will have better performance compared to cross-language object graph serialization.

### Quick Start
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

### Class Registration
`FuryBuilder#requireClassRegistration`/`FuryBuilder#withSecureMode` can be used to disable class registration, this will allow to deserialize objects unknown types, more flexible but less secure. Do not disable class registration until you know what you are doing.

Class registration can not only reduce security risks, but also avoid classname serialization cost.

### Advanced Fury Creation
Single thread fury:
```java
Fury fury = Fury.builder()
  .withLanguage(Language.JAVA)
  // enable referecne tracking for shared/circular reference.
  // Disable it will have better performance if no duplciate reference.
  .withRefTracking(true)
  // compress int/long for smaller size
  // .withNumberCompressed(true)
  .withCompatibleMode(CompatibleMode.SCHEMA_CONSISTENT)
  // enable type forward/backward compatibility
  // disable it for small size and better performance.
  // .withCompatibleMode(CompatibleMode.COMPATIBLE)
  // enable async multi-threaded compilation.
  .withAsyncCompilationEnabled(true)
  .build();
byte[] bytes = fury.serialize(object);
System.out.println(fury.deserialize(bytes));
```
Thread-safe fury:
```java
ThreadSafeFury fury = Fury.builder()
  .withLanguage(Language.JAVA)
  // enable referecne tracking for shared/circular reference.
  // Disable it will have better performance if no duplciate reference.
  .withRefTracking(true)
  // compress int/long for smaller size
  // .withNumberCompressed(true)
  .withCompatibleMode(CompatibleMode.SCHEMA_CONSISTENT)
  // enable type forward/backward compatibility
  // disable it for small size and better performance.
  // .withCompatibleMode(CompatibleMode.COMPATIBLE)
  // enable async multi-threaded compilation.
  .withAsyncCompilationEnabled(true)
  .buildThreadSafeFury();
byte[] bytes = fury.serialize(object);
System.out.println(fury.deserialize(bytes));
```

### Zero-Copy Serialization
```java
import io.fury.*;
import io.fury.serializers.BufferObject;
import io.fury.memory.MemoryBuffer;
import java.util.*;
import java.util.stream.Collectors;

public class ZeroCopyExample {
  // mvn exec:java -Dexec.mainClass="io.ray.fury.examples.ZeroCopyExample"
  public static void main(String[] args) {
    // Note that fury instance should be reused instead of creation every time.
    Fury fury = Fury.builder()
        .withLanguage(Language.JAVA)
        .build();
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
Fury supports share type metadata (class name, field name, final field type information, etc.) between multiple serializations in a context (ex. TCP connection), and this information will be sent to the peer during the first serialization in the context. Based on this metadata, the peer can rebuild the same deserializer, which avoids transmitting metadata for subsequent serializations and reduces network traffic pressure and supports type forward/backward compatibility automatically.

```java
// Fury.builder()
//   .withLanguage(Language.JAVA)
//   .withReferenceTracking(true)
//   // share meta across serialization.
//   .withMetaContextShareEnabled(true)
// Not thread-safe fury.
MetaContext context = xxx;
fury.getSerializationContext().setMetaContext(context);
byte[] bytes = fury.serialize(o);
// Not thread-safe fury.
MetaContext context = xxx;
fury.getSerializationContext().setMetaContext(context);
fury.deserialize(bytes)

// Thread-safe fury
fury.setClassLoader(beanA.getClass().getClassLoader());
byte[] serialized = fury.execute(
  f -> {
    f.getSerializationContext().setMetaContext(context);
    return f.serialize(beanA);
  });
// thread-safe fury
fury.setClassLoader(beanA.getClass().getClassLoader());
Object newObj = fury.execute(
  f -> {
    f.getSerializationContext().setMetaContext(context);
    return f.deserialize(serialized);
  });
```

## Row format protocol
### Java
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
### Python
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
print(f"start: {datetime.datetime.now()}")
foo_row = pyfury.RowData(encoder.schema, binary)
print(foo_row.f2[100000], foo_row.f4[100000].f1, foo_row.f4[200000].f2[5])
print(f"end: {datetime.datetime.now()}")

binary = pickle.dumps(foo)
print(f"pickle start: {datetime.datetime.now()}")
new_foo = pickle.loads(binary)
print(new_foo.f2[100000], new_foo.f4[100000].f1, new_foo.f4[200000].f2[5])
print(f"pickle end: {datetime.datetime.now()}")
```
### Apache Arrow Support
Fury Format also supports automatic conversion from/to Arrow Table/RecordBatch.

Java:
```java
Schema schema = TypeInference.inferSchema(BeanA.class);
ArrowWriter arrowWriter = ArrowUtils.createArrowWriter(schema);
Encoder<BeanA> encoder = Encoders.rowEncoder(BeanA.class);
for (int i = 0; i < 10; i++) {
  BeanA beanA = BeanA.createBeanA(2);
  arrowWriter.write(encoder.toRow(beanA));
}
return arrowWriter.finishAsRecordBatch();
```
Python:
```python
import pyfury
encoder = pyfury.encoder(Foo)
encoder.to_arrow_record_batch([foo] * 10000)
encoder.to_arrow_table([foo] * 10000)
```
C++
```c++
std::shared_ptr<ArrowWriter> arrow_writer;
EXPECT_TRUE(
    ArrowWriter::Make(schema, ::arrow::default_memory_pool(), &arrow_writer)
        .ok());
for (auto &row : rows) {
  EXPECT_TRUE(arrow_writer->Write(row).ok());
}
std::shared_ptr<::arrow::RecordBatch> record_batch;
EXPECT_TRUE(arrow_writer->Finish(&record_batch).ok());
EXPECT_TRUE(record_batch->Validate().ok());
EXPECT_EQ(record_batch->num_columns(), schema->num_fields());
EXPECT_EQ(record_batch->num_rows(), row_nums);
```
```java
Schema schema = TypeInference.inferSchema(BeanA.class);
ArrowWriter arrowWriter = ArrowUtils.createArrowWriter(schema);
Encoder<BeanA> encoder = Encoders.rowEncoder(BeanA.class);
for (int i = 0; i < 10; i++) {
  BeanA beanA = BeanA.createBeanA(2);
  arrowWriter.write(encoder.toRow(beanA));
}
return arrowWriter.finishAsRecordBatch();
```
