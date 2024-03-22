# Cross language object graph serialization

Fury xlang serialization is an automatic object serialization framework that supports reference and polymorphism.
Fury will convert an object from/to fury xlang serialization binary format.
Fury has two core concepts for xlang serialization:

- **Fury xlang binary format**
- **Framework implemented in different languages to convert object to/from Fury xlang binary format**

The serialization format is a dynamic binary format. The dynamics and reference/polymorphism support make Fury flexible,
much more easy to use, but
also introduce more complexities compared to static serialization frameworks. So the format will be more complex.

## Type Systems

### Data Types

- bool: A boolean value (true or false).
- byte: An 8-bit signed integer.
- i16: A 16-bit signed integer.
- i32: A 32-bit signed integer.
- i64: A 64-bit signed integer.
- half-float: A 16-bit floating point number.
- float: A 32-bit floating point number.
- double: A 64-bit floating point number including NaN and Infinity.
- string: A text string encoded using Latin1/UTF16/UTF-8 encoding.
- enum: a data type consisting of a set of named values. Rust enum with non-predefined field values are not supported as
  an enum
- list: A sequence of objects.
- set: An unordered set of unique elements.
- map: A map of key-value pairs.
- time types:
    - Duration: an absolute length of time independent of any calendar/timezone, as a count of seconds and
      fractions of seconds at nanosecond resolution.
    - Timestamp: a point in time independent of any calendar/timezone, as a count of seconds and fractions of
      seconds at nanosecond resolution. The count is relative to an epoch at UTC midnight on January 1, 1970.
- decimal: exact decimal value represented as an integer value in two's complement.
- binary: binary data.
- array type: only allow numeric component. Other arrays will be taken as List. The implementation should support the
  interoperability between array and list.
    - array: multiple dimension array which every subarray can have have different size.
    - int16_array: one dimension int16 array.
    - int32_array: one dimension int32 array.
    - int64_array: one dimension int64 array.
    - half_float_array: one dimension half_float_16 array.
    - float_array: one dimension float32 array.
    - double_array: one dimension float64 array.
- tensor: a multidimensional dense array of fixed-size values such as a NumPy ndarray.
- sparse tensor: a multidimensional array whose elements are almost all zeros.
- arrow record batch: an arrow [record batch](https://arrow.apache.org/docs/cpp/tables.html#record-batches) object.
- arrow table: an arrow [table](https://arrow.apache.org/docs/cpp/tables.html#tables) object.

### Type ambiguities

Due to differences between type systems of languages, those types can't be mapped one-to-one between languages. When
deserializing, Fury use the target data structure type and the data type in the data jointly to determine how to
deserialize and populate the target data structure. For example:

```java
class Foo {
  int[] intArray;
  Object[] objectArray;
  List<Object> objectList;
}
```

`intArray` has `int32_array` type. But both `objectArray` and `objectList` field in the serialize data have `list` data
type. When deserializing, the implementation will create an `Object` array for `objectArray`, but create a `ArrayList`
for `objectList` to populate it's elements.

### Type ID

All internal data types are expressed using unsigned ID `-64~-1`. Users can use `0~32703` for representing their types.
At runtime, all type ids are added by `64`, represented and encoded as an unsigned int.

### Type mapping

See [Type mapping](../guide/xlang_type_mapping.md)

## Spec overview

Here is the overall format:

```
| fury header | object ref meta | object type meta | object value data |
```

The data are serialized using little endian byte order overall. If bytes swap is costly for some object,
Fury will write the byte order for that object into the data instead of converting it to little endian.

## Fury header

Fury header consists starts one byte:

```
|     4 bits    | 1 bit | 1 bit | 1 bit  | 1 bit |          optional 4 bytes          |
+---------------+-------+-------+--------+-------+------------------------------------+
| reserved bits |  oob  | xlang | endian | null  | unsigned int for meta start offset |
```

- null flag: 1 when object is null, 0 otherwise. If an object is null, other bits won't be set.
- endian flag: 1 when data is encoded by little endian, 0 for big endian.
- xlang flag: 1 when serialization uses xlang format, 0 when serialization uses Fury java format.
- oob flag: 1 when passed `BufferCallback` is not null, 0 otherwise.

If meta share mode is enabled, an uncompressed unsigned int is appended to indicate the start offset of metadata.

## Reference Meta

Reference tracking handles whether the object is null, and whether to track reference for the object by writing
corresponding flags and maintaining internal state.

Reference flags:

| Flag                | Byte Value | Description                                                                                                                                             |
|---------------------|------------|---------------------------------------------------------------------------------------------------------------------------------------------------------|
| NULL FLAG           | `-3`       | This flag indicates the object is a null value. We don't use another byte to indicate REF, so that we can save one byte.                                |
| REF FLAG            | `-2`       | This flag indicates the object is already serialized previously, and fury will write a ref id with unsigned varint format instead of serialize it again |
| NOT_NULL VALUE FLAG | `-1`       | This flag indicates the object is a non-null value and fury doesn't track ref for this type of object.                                                  |
| REF VALUE FLAG      | `0`        | This flag indicates the object is referencable and the first time to serialize.                                                                         |

When reference tracking is disabled globally or for specific types, or for certain types within a particular
context(e.g., a field of a type), only the `NULL` and `NOT_NULL VALUE` flags will be used for reference meta.

For languages which doesn't support reference such as rust, reference tracking must be disabled for correct
deserialization by fury rust implementation.

For languages whose object values are not null by default:

- In rust, Fury takes `Option:None` as a null value
- In c++, Fury takes `std::nullopt` as a null value
- In golang, Fury takes `null interface/pointer` as a null value

If one want to deserialize in languages like `Java/Python/JavaScript`, he should mark the type with all fields
not-null by default, or using schema-evolution mode to carry the not-null fields info in the data.

## Type Meta

For every type to be serialized, it must be registered with an optional ID first. The registered type will have a
user-provided or an auto-growing unsigned int i.e. `type_id`. The registration can be used for security check and type
identification. The id of user registered type will be added by `64` to make space for Fury internal data types.

Depending on whether meta share mode and registration is enabled for current type, Fury will write type meta
differently.

### Schema consistent

If schema consistent mode is enabled globally or enabled for current type, type meta will be written as a fury unsigned
varint of `type_id`.

### Schema evolution

If schema evolution mode is enabled globally or enabled for current type, type meta will be written as follows:

- If meta share mode is not enabled, type meta will be written as schema consistent mode. Additionally, field meta such
  as field type and name will be written with the field value using a key-value like layout.
- If meta share mode is enabled, type meta will be written as a meta-share encoded binary if type hasn't been written
  before, otherwise an unsigned varint id which references to previous written type meta will be written.

## Meta share

> This mode will forbid streaming writing since it needs to look back for update the start offset after the whole object
> graph
> writing and meta collecting is finished. Only in this way we can ensure deserialization failure doesn't lost shared
> meta.
> Meta streamline will be supported in the future for enclosed meta sharing which doesn't cross multiple serializations
> of different objects.

For Schema consistent mode, type will be encoded as an enumerated string by full type name. Here we mainly describe
the meta layout for schema evolution mode:

```
|      8 bytes meta header      |   variable bytes   |  variable bytes   | variable bytes |
+-------------------------------+--------------------+-------------------+----------------+
| 7 bytes hash + 1 bytes header |  current type meta |  parent type meta |      ...       |
```

Type meta are encoded from parent type to leaf type, only type with serializable fields will be encoded.

### Meta header

Meta header is a 64 bits number value encoded in little endian order.

- Lowest 4 digits `0b0000~0b1110` are used to record num classes. `0b1111` is preserved to indicate that Fury need to
  read more bytes for length using Fury unsigned int encoding. If current type doesn't has parent type, or parent
  type doesn't have fields to serialize, or we're in a context which serialize fields of current type
  only, num classes will be 1.
- 5rd bit is used to indicate whether this type needs schema evolution.
- Other 56 bits is used to store the unique hash of `flags + all layers type meta`.
- Inheritance: Fury
    - For languages

### Single layer type meta

```
| unsigned varint | var uint |  field info: variable bytes   | variable bytes  | ... |
+-----------------+----------+-------------------------------+-----------------+-----+
|   num_fields    | type id  | header + type id + field name | next field info | ... |
```

- num fields: encode `num fields` as unsigned varint.
    - If current type is schema consistent, then num_fields will be `0` to flag it.
    - If current type isn't schema consistent, then num_fields will be the number of compatible fields. For example,
      users can use tag id to mark some field as compatible field in schema consistent context. In such cases, schema
      consistent fields will be serialized first, then compatible fields will be serialized next. At deserialization,
      Fury will use fields info of those fields which aren't annotated by tag id for deserializing schema consistent
      fields, then use fields info in meta for deserializing compatible fields.
- Field info:
    - Header(8 bits):
        - Format:
            - `reserved 1 bit + 3 bits field name encoding + polymorphism flag + nullability flag + ref tracking flag + tag id flag`.
        - Users can use annotation to provide those info.
            - tag id: when set to 1, field name will be written by an unsigned varint tag id.
            - ref tracking: when set to 0, ref tracking will be disabled for this field.
            - nullability: when set to 0, this field won't be null.
            - polymorphism: when set to 1, the actual type of field will be the declared field type even the type if
              not `final`.
            - 3 bits field name encoding will be set to meta string encoding flags when tag id is not set.
    - Type id:
        - For registered type-consistent classes, it will be the registered type id.
        - Otherwise it will be encoded as `OBJECT_ID` if it isn't `final` and `FINAL_OBJECT_ID` if it's `final`. The
          meta
          for such types is written separately instead of inlining here is to reduce meta space cost if object of this
          type is serialized in current object graph multiple times, and the field value may be null too.
    - List Type Info: this type will have an extra byte for elements info.
      Users can use annotation to provide those info.
        - elements type same
        - elements tracking ref
        - elements nullability
        - elements declared type
    - Map Type Info: this type will have an extra byte for kv items info.
      Users can use annotation to provide those info.
        - keys type same
        - keys tracking ref
        - keys nullability
        - keys declared type
        - values type same
        - values tracking ref
        - values nullability
        - values declared type
    - Field name: If tag id is set, tag id will be used instead. Otherwise meta string encoding length and data will
      be written instead.

Field order are left as implementation details, which is not exposed to specification, the deserialization need to
resort fields based on Fury field comparator. In this way, fury can compute statistics for field names or types and
using a more compact encoding.

### Other layers type meta

Same encoding algorithm as the previous layer.

## Meta String

Meta string is mainly used to encode meta strings such as field names.

### Encoding Algorithms

String binary encoding algorithm:

| Algorithm                 | Pattern        | Description                                                                                                                                      |
|---------------------------|----------------|--------------------------------------------------------------------------------------------------------------------------------------------------|
| LOWER_SPECIAL             | `a-z._$\|`     | every char is written using 5 bits, `a-z`: `0b00000~0b11001`, `._$\|`: `0b11010~0b11101`                                                         |
| LOWER_UPPER_DIGIT_SPECIAL | `a-zA-Z0~9._$` | every char is written using 6 bits, `a-z`: `0b00000~0b11110`, `A-Z`: `0b11010~0b110011`, `0~9`: `0b110100~0b111101`, `._$`: `0b111110~0b1000000` |
| UTF-8                     | any chars      | UTF-8 encoding                                                                                                                                   |

Encoding flags:

| Encoding Flag             | Pattern                                                   | Encoding Algorithm                                                                                                                  |
|---------------------------|-----------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| LOWER_SPECIAL             | every char is in `a-z._$\|`                               | `LOWER_SPECIAL`                                                                                                                     |
| REP_FIRST_LOWER_SPECIAL   | every char is in `a-z._$` except first char is upper case | replace first upper case char to lower case, then use `LOWER_SPECIAL`                                                               |
| REP_MUL_LOWER_SPECIAL     | every char is in `a-zA-Z._$`                              | replace every upper case char by `\|` + `lower case`, then use `LOWER_SPECIAL`, use this encoding if it's smaller than Encoding `3` |
| LOWER_UPPER_DIGIT_SPECIAL | every char is in `a-zA-Z._$`                              | use `LOWER_UPPER_DIGIT_SPECIAL` encoding if it's smaller than Encoding `2`                                                          |
| UTF8                      | any utf-8 char                                            | use `UTF-8` encoding                                                                                                                |
| Compression               | any utf-8 char                                            | lossless compression                                                                                                                |

Depending on cases, one can choose encoding `flags + data` jointly, uses 3 bits of first byte for flags and other bytes
for data.

## Value Format

### Basic types

#### Bool

- size: 1 byte
- format: 0 for `false`, 1 for `true`

#### Byte

- size: 1 byte
- format: write as pure byte.

#### Short

- size: 2 byte
- byte order: little endian order

#### Unsigned int

- size: 1~5 byte
- Format: The most significant bit (MSB) in every byte indicates whether to have the next byte. If first bit is set
  i.e. `b & 0x80 == 0x80`, then
  the next byte should be read until the first bit of the next byte is unset.

#### Signed int

- size: 1~5 byte
- Format: First convert the number into positive unsigned int by `(v << 1) ^ (v >> 31)` ZigZag algorithm, then encoding
  it as an unsigned int.

#### Unsigned long

- size: 1~9 byte
- Fury PVL(Progressive Variable-length Long) Encoding:
    - positive long format: first bit in every byte indicates whether to have the next byte. If first bit is set
      i.e. `b & 0x80 == 0x80`, then the next byte should be read until the first bit is unset.

#### Signed long

- size: 1~9 byte
- Fury SLI(Small long as int) Encoding:
    - If long is in [-1073741824, 1073741823], encode as 4 bytes int: `| little-endian: ((int) value) << 1 |`
    - Otherwise write as 9 bytes: `| 0b1 | little-endian 8 bytes long |`
- Fury PVL(Progressive Variable-length Long) Encoding:
    - First convert the number into positive unsigned long by ` (v << 1) ^ (v >> 63)` ZigZag algorithm to reduce cost of
      small negative numbers, then encoding it as an unsigned long.

#### Float

- size: 4 byte
- format: encode the specified floating-point value according to the IEEE 754 floating-point "single format" bit layout,
  preserving Not-a-Number (NaN) values, then write as binary by little endian order.

#### Double

- size: 8 byte
- format: encode the specified floating-point value according to the IEEE 754 floating-point "double format" bit layout,
  preserving Not-a-Number (NaN) values. then write as binary by little endian order.

### String

Format:

```
| header: size << 2 | 2 bits encoding flags | binary data |
```

- `size + encoding` will be concat as a long and encoded as an unsigned var long. The little 2 bits is used for
  encoding:
  0 for `latin`, 1 for `utf-16`, 2 for `utf-8`.
- encoded string binary data based on encoding: `latin/utf-16/utf-8`.

Which encoding to choose:

- For JDK8: fury detect `latin` at runtime, if string is `latin` string, then use `latin` encoding, otherwise
  use `utf-16`.
- For JDK9+: fury use `coder` in `String` object for encoding, `latin`/`utf-16` will be used for encoding.
- If the string is encoded by `utf-8`, then fury will use `utf-8` to decode the data. But currently fury doesn't enable
  utf-8 encoding by default for java. Cross-language string serialization of fury uses `utf-8` by default.

### Enum

Enum will be serialized as an unsigned varint of the ordinal.

### List

Format:

```
length(unsigned varint) | elements header | elements data
```

#### Elements header

In most cases, all elements are same type and not null, elements header will encode those homogeneous
information to avoid the cost of writing it for every element. Specifically, there are four kinds of information
which will be encoded by elements header, each use one bit:

- If track elements ref, use the first bit `0b1` of the header to flag it.
- If the elements has null, use the second bit `0b10` of the header to flag it. If ref tracking is enabled for this
  element type, this flag is invalid.
- If the element types are not declared type, use the 3rd bit `0b100` of the header to flag it.
- If the element types are different, use the 4rd bit `0b1000` header to flag it.

By default, all bits are unset, which means all elements won't track ref, all elements are same type, not null and
the actual element is the declared type in the custom type field.

#### Elements data

Based on the elements header, the serialization of elements data may skip `ref flag`/`null flag`/`element type info`.

`CollectionSerializer#write/read` can be taken as an example.

### Array

#### Primitive array

Primitive array are taken as a binary buffer, serialization will just write the length of array size as an unsigned int,
then copy the whole buffer into the stream.

Such serialization won't compress the array. If users want to compress primitive array, users need to register custom
serializers for such types.

#### Object array

Object array is serialized using the list format. Object component type will be taken as list element
generic type.

### Map

> All Map serializers must extend `AbstractMapSerializer`.

Format:

```
| length(unsigned varint) | key value chunk data | ... | key value chunk data |
```

#### Map Key-Value data

Map iteration is too expensive, Fury won't compute the header like for list since it introduce
[considerable overhead](https://github.com/apache/incubator-fury/issues/925).
Users can use `MapFieldInfo` annotation to provide header in advance. Otherwise Fury will use first key-value pair to
predict header optimistically, and update the chunk header if the prediction failed at some pair.

Fury will serialize map chunk by chunk, every chunk has 127 pairs at most.

```
|    1 byte      |     1 byte     | variable bytes  |
+----------------+----------------+-----------------+
| chunk size: N  |    KV header   |   N*2 objects   |
```

KV header:

- If track key ref, use the first bit `0b1` of the header to flag it.
- If the key has null, use the second bit `0b10` of the header to flag it. If ref tracking is enabled for this
  key type, this flag is invalid.
- If the key types of map are different, use the 3rd bit `0b100` of the header to flag it.
- If the actual key type of map is not the declared key type, use the 4rd bit `0b1000` of the header to flag it.
- If track value ref, use the 5th bit `0b10000` of the header to flag it.
- If the value has null, use the 6th bit `0b100000` of the header to flag it. If ref tracking is enabled for this
  value type, this flag is invalid.
- If the value types of map are different, use the 7rd bit `0b1000000` header to flag it.
- If the value type of map is not the declared value type, use the 8rd bit `0b10000000` of the header to flag it.

If streaming write is enabled, which means Fury can't update written `chunk size`. In such cases, map key-value data
format will be:

```
|    1 byte      | variable bytes  |
+----------------+-----------------+
|    KV header   |   N*2 objects   |
```

`KV header` will be a header marked by `MapFieldInfo` in java. For languages such as golang, this can be computed in
advance for non-interface types in most times.

### Enum

Enums are serialized as an unsigned var int. If the order of enum values change, the deserialized enum value may not be
the value users expect. In such cases, users must register enum serializer by make it write enum value as an enumerated
string with unique hash disabled.

### Decimal

Not supported for now.

### Object

Object means object of `pojo/struct/bean/record` type.
Object will be serialized by writing its fields data in fury order.

Depending on schema compatibility, objects will have different formats.

#### Field order

Field will be ordered as following, every group of fields will have its own order:

- primitive fields: larger size type first, smaller later, variable size type last.
- boxed primitive fields: same order as primitive fields
- final fields: same type together, then sorted by field name lexicographically.
- list fields: same order as final fields
- map fields: same order as final fields
- other fields: same order as final fields

#### Schema consistent

Object fields will be serialized one by one using following format:

```
Primitive field value:
|   var bytes    |
+----------------+
|   value data   |
+----------------+
Boxed field value:
| one byte  |   var bytes   |
+-----------+---------------+
| null flag |  field value  |
+-----------+---------------+
field value of final type with ref tracking:
| var bytes | var objects |
+-----------+-------------+
| ref meta  | value data  |
+-----------+-------------+
field value of final type without ref tracking:
| one byte  | var objects |
+-----------+-------------+
| null flag | field value |
+-----------+-------------+
field value of non-final type with ref tracking:
| one byte  | var bytes | var objects |
+-----------+-------------+-------------+
| ref meta  | type meta  | value data  |
+-----------+-------------+-------------+
field value of non-final type without ref tracking:
| one byte  | var bytes | var objects |
+-----------+------------+------------+
| null flag | type meta | value data |
+-----------+------------+------------+
```

#### Schema evolution

Schema evolution have similar format as schema consistent mode for object except:

- For this object type itself, `schema consistent` mode will write type by id/name, but `schema evolution` mode will
  write type field names, types and other meta too, see [Type meta](#type-meta).
- Type meta of `final custom type` needs to be written too, because peers may not have this type defined.

### Type

Type will be serialized using type meta format.

## Implementation guidelines

- Try to merge multiple bytes into an int/long write before writing to reduce memory IO and bound check cost.
- Read multiple bytes as an int/long, then split into multiple bytes to reduce memory IO and bound check cost.
- Try to use one varint/long to write flags and length together to save one byte cost and reduce memory io.
- Condition branches are less expensive compared to memory IO cost unless there are too many branches.
