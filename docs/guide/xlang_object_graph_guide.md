## Cross-language object graph serialization

### Serialize built-in types
Common types can be serialized automatically: primitive numeric types, string, binary, array, list, map and so on.


**Java**

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

**Python**

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

**Golang**

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

**JavaScript**

```javascript
// Coming soon
```

### Serialize custom types
Serializing user-defined types needs registering the custom type using the register API to establish the mapping relationship between the type in different languages.

**Java**

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

**Python**

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

**Golang**

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

**JavaScript**

```javascript
// Coming soon
```

### Serialize Shared Reference and Circular Reference
Shared reference and circular reference can be serialized automatically, no duplicate data or recursion error.

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

**JavaScript**

```javascript
// Coming soon
```

### Zero-Copy Serialization

**Java**

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

**Python**

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

**Golang**

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

**JavaScript**

```javascript
// Coming soon
```
