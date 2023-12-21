# Fury Java Serialization Specification

## Spec overview

Fury Java Serialization is an automatic object serialization framework that supports reference and polymorphism. Fury
will
convert an object from/to fury java serialization binary format. Fury has two core concepts for java serialization:

- **Fury Java Binary format**
- Framework to convert object to/from Fury Java Binary format

The serialization format is a dynamic binary format. The dynamics and reference/polymorphism support make Fury flexible,
much more easy to use, but
also introduce more complexities compared to static serialization frameworks. So the format will be more complex.

Here is the overall format:

```
| fury header | object ref meta | object class meta | object value data |
```

The data are serialized using little endian order overall. If bytes swap is costly, the byte order will be encoded as a
flag in data.

## Fury header

Fury header consists starts one byte:

```
| resvered 4 bits | oob | xlang | endian | null |
```

- null flag: set when object is null, unset otherwise. If object is null, other bits won't be set.
- endian flag: set when system use little endian, unset otherwise.
- xlang flag: set when serialization uses xlang format, unset when serialization use Fury java format.
- oob flag: set when passed `BufferCallback` is not null, unset otherwise.

If meta share mode is enabled, uncompressed little-endian 4 bytes is appended to indicate the start offset of meta data.

## Reference Meta

Reference tracking handles whether the object is null, and whether to track reference for the object by writing
corresponding flags and maintain internal state.

Reference flags:

| Flag                | Byte Value | Description                                                                                                                   |
|---------------------|------------|-------------------------------------------------------------------------------------------------------------------------------|
| NULL FLAG           | `-3`       | This flag indicates that object is a not-null value. We don't use another byte to indicate REF, so that we can save one byte. |
| REF FLAG            | `-2`       | this flag indicates the object is written before, and fury will write a unsigned ref id instead of serialize it again         |
| NOT_NULL VALUE FLAG | `-1`       | this flag indicates that the object is a non-null value and fury doesn't track ref for this type of object.                   |
| REF VALUE FLAG      | `0`        | this flag indicates that the object is a referencable and first read.                                                         |

When reference tracking is disabled globally or only for some type, or for some type under some context such as some
field of a class, only `NULL FLAG` and ` NOT_NULL VALUE FLAG` will be used.

## Class Meta

Depending on whether meta share mode is enabled, Fury will write class meta differently.

### Schema consistent

If schema consistent mode is enabled globally or enabled for current class, class meta will be written as follows:

- If class is registered, it will be written as a little-endian unsigned int: `class_id << 1` using fury unsigned int
  format.
- If class is not registered, fury will write one byte `0b01/0b11` first, then write class name.
    - The higher bit will be 1 if the class is an
      array, and written class will be the component class. This can reduce array class name cost if component class is
      serialized before.
    - The little bit is different first bit of
      encoded class id, which is `0`. Fury can use this information to determine whether read class by class id.
    - If meta share mode is enabled, class will be written as a unsigned int.
    - If meta share mode is not enabled, class will be written as two enumerated string:
        - package name.
        - class name.

### Schema evolution

If schema evolution mode is enabled globally or enabled for current class, class meta will be written as follows:

- If meta share mode is not enabled, class meta will be written as scheme consistent mode, field meta such as field type
  and name will be written when the object value is being serialized using a key-value like layout.
- If meta share mode is enabled, class will be written as a unsigned int.

## Meta share

> This mode will forbid streaming writing since it needs to look back for update the offset after the whole object graph
> writing and mete collecting is finished.
> We have plan to streamline meta writing but haven't started yet.

### Schema consistent

Class will be encoded as an enumerated string by full class name.

### Schema evolution

Class meta format:

```
| meta header: hash + num classes | current class meta | parent class meta | ... |
```

#### Meta header

Meta header is a 64 bits number value encoded in little endian order.

- Lowest 4 digits `0b0000~0b1111` are used to record num classes. `0b1111` is preserved to indicate that Fury need to
  read more bytes for length using Fury unsigned int encoding. If current class doesn't has parent class, or parent
  class doesn't have fields to serialize, or we're in a context which serialize fields of current class
  only( `ObjectStreamSerializer#SlotInfo` is an example),
- Other 60 bits is used to store murmur hash of `flags + all layers class meta`. num classes will be 0.

#### Single layer class meta

```
| enumerated class name string | unsigned int: num fields | field info: type info + field name | next field info | ... |
```

Type info of custom type field will be written as an one-byte flag instead of inline its meta, because the field value may be null, and Fury can reduce this field type meta writing if object of this type is serialized to in current object graph.

Field order are left as implementation details, which is not exposed to specification, the deserialization need to
resort fields based on Fury field comparator. In this way, fury can compute statistics for field names or types and
using a more compact encoding.

## Enumerated String

Enumerated string are mainly used to encode meta string such class name and field names. The format consists of header
and binary.

Header are written using little endian order, Fury can read this flag first to determine how to deserialize the data.

### Header

#### Write by data

If string hasn't been written before, the data will be written as follows:

```
| unsigned int: string binary size + 1bit: not written before | 61bits: murmur hash + 3 bits encoding flags | string binary |
```

Murmur hash can be omitted if caller pass a flag. In such cases, the format will be:

```
| unsigned int: string binary size + 1bit: not written before | 8 bits encoding flags | string binary |
```

5 bits in `8 bits encoding flags` will be left empty.

Encoding flags:

| Encoding Flag | Pattern                                                   | Encoding Action                                                                                                                     |
|---------------|-----------------------------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| 0             | every char is in `a-z._$\|`                               | `LOWER_SPECIAL`                                                                                                                     |
| 1             | every char is in `a-z._$` except first char is upper case | replace first upper case char to lower case, then use `LOWER_SPECIAL`                                                               |
| 2             | every char is in `a-zA-Z._$`                              | replace every upper case char by `\|` + `lower case`, then use `LOWER_SPECIAL`, use this encoding if it's smaller than Encoding `3` |
| 3             | every char is in `a-zA-Z._$`                              | use `LOWER_UPPER_DIGIT_SPECIAL` encoding if it's smaller than Encoding `2`                                                          |
| 4             | any utf-8 char                                            | use `UTF-8` encoding                                                                                                                |

#### Write by ref

If string has been written before, the data will be written as follows:

```
| unsigned int: written string id + 1bit: written before |
```

### String binary

String binary encoding:

| Algorithm                 | Pattern        | Description                                                                                                                                     |
|---------------------------|----------------|-------------------------------------------------------------------------------------------------------------------------------------------------|
| LOWER_SPECIAL             | `a-z._$\|`     | every char is writen using 5 bits, `a-z`: `0b00000~0b11001`, `._$\|`: `0b11010~0b11101`                                                         |
| LOWER_UPPER_DIGIT_SPECIAL | `a-zA-Z0~9._$` | every char is writen using 6 bits, `a-z`: `0b00000~0b11110`, `A-Z`: `0b11010~0b110011`, `0~9`: `0b110100~0b111101`, `._$`: `0b111110~0b1000000` |
| UTF-8                     | any chars      | UTF-8 encoding                                                                                                                                  |

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

#### Char

- size: 2 byte
- byte order: little endian order

#### Unsigned int

- size: 1~5 byte
- Format: first bit in every byte indicate whether to has next byte. if first bit is set i.e. `b & 0x80 == 0x80`, then
  next byte should be read util first bit of next byte is unset.

#### Signed int

- size: 1~5 byte
- Format: First convert the number into positive unsigned int by `(v << 1) ^ (v >> 31)` ZigZag algorithm, then encoding
  it as an unsigned int.

#### Unsigned long

- size: 1~9 byte
- Fury PVL(Progressive Variable-length Long) Encoding:
    - positive long format: first bit in every byte indicate whether to has next byte. if first bit is set
      i.e. `b & 0x80 == 0x80`, then next byte should be read util first bit is unset.

#### Signed long

- size: 1~9 byte
- Fury SLI(Small long as int) Encoding:
    - If long is in [-1073741824, 1073741823], encode as 4 bytes int: `| little-endian: ((int) value) << 1 |`
    - Otherwise write as 9 bytes: `| 0b1 | little-endian 8bytes long |`
- Fury PVL(Progressive Variable-length Long) Encoding:
    - First convert the number into positive unsigned long by ` (v << 1) ^ (v >> 63)` ZigZag algorithm to reduce cost of
      small negative numbers, then encoding it as an unsigned long.

#### Float

- size: 4 byte
- format: convert float to 4 bytes int by `Float.floatToRawIntBits`, then write as binary by little endian order.

#### Double

- size: 8 byte
- format: convert double to 8 bytes int by `Double.doubleToRawLongBits`, then write as binary by little endian order.

### String

Format:

```
| header: size + encoding | binary data |
```

- `size + encoding` will be concat as a long and encoded as a unsigned var long. The little 2 bits is used for encoding:
  0 for `latin`, 1 for `utf-16`, 2 for `utf-8`.
- encoded string binary data based on encoding: `latin/utf-16/utf-8`.

Which encoding to choose:

- For JDK8: fury detect `latin` at runtime, if string is `latin` string, then use `latin` encoding, otherwise
  use `utf-16`.
- For JDK9+: fury use `coder` in `String` object for encoding, `latin`/`utf-16` will be used for encoding.
- If the string is encoded by `utf-8`, then fury will use `utf-8` to decode the data. But currently fury doesn't enable
  utf-8 encoding by default for java. Cross-language string serialization of fury use `utf-8` by default.

### Collection

> All collection serializer must extends `AbstractCollectionSerializer`.

Format:

```
length(unsigned varint) | collection header | elements header | elements data
```

#### Collection header

- For `ArrayList/LinkedArrayList/HashSet/LinkedHashSet`, this will be empty.
- For `TreeSet`, this will be `Comparator`
- For subclass of `ArrayList`, this may be extra object field info.

#### Elements header

In most cases, all collection elements are same type and not null, elements header will encode those homogeneous
information to avoid the cost of writing it for every elements. Specifically, there are four kinds of information
which will be encoded by elements header, each use one bit:

- If track elements ref, use first bit `0b1` of header to flag it.
- If collection has null, use second bit `0b10` of header to flag it. If ref tracking is enabled for this
  element type, this flag is invalid.
- If collection elements type is not declare type, use 3rd bit `0b100` of header to flag it.
- If collection elements type different, use 4rd bit `0b1000` of header to flag it.

By default, all bits are unset, which means all elements won't track ref, all elements are same type,, not null and the
actual element is the declare type in custom class field.

#### Elements data

Based on the elements header, the serialization of elements data may skip `ref flag`/`null flag`/`element class info`.

`CollectionSerializer#write/read` can be taken as an example.

### Array

#### Primitive array

Primitive array are taken as a binary buffer, serialization will just write the length of array size as an unsigned int,
then copy the whole buffer into the stream.

Such serialization won't compress the array. If users want to compress primitive array, users need to register custom
serializers for such types.

#### Object array

Object array is serialized using collection format. Object component type will be taken as collection element generic
type.

### Map

> All Map serializer must extends `AbstractMapSerializer`.

Format:

```
| length(unsigned varint) | map header | key value pairs data |
```

#### Map header

- For `HashMap/LinkedHashMap`, this will be empty.
- For `TreeMap`, this will be `Comparator`
- For other `Map`, this may be extra object field info.

#### Map Key-Value data

Map iteration is too expensive, Fury can't compute the header like for collection before since it introduce
[considerable overhead](https://github.com/alipay/fury/issues/925).
Users can use `MapFieldInfo` annotation to provide header in advance. Otherwise Fury will use first key-value pair to
predict header optimistically, and update the chunk header if predict failed at some pair.

Fury will serialize map chunk by chunk, every chunk
has 127 pairs at most.

```
+----------------+----------------+~~~~~~~~~~~~~~~~~+
| chunk size: N  |    KV header   |   N*2 objects   |
+----------------+----------------+~~~~~~~~~~~~~~~~~+
```

KV header:

- If track key ref, use first bit `0b1` of header to flag it.
- If key has null, use second bit `0b10` of header to flag it. If ref tracking is enabled for this
  key type, this flag is invalid.
- If map key type is not declared type, use 3rd bit `0b100` of header to flag it.
- If map key type different, use 4rd bit `0b1000` of header to flag it.
- If track value ref, use 5rd bit `0b10000` of header to flag it.
- If value has null, use 6rd bit `0b100000` of header to flag it. If ref tracking is enabled for this
  value type, this flag is invalid.
- If map value type is not declared type, use 7rd bit `0b1000000` of header to flag it.
- If map value type different, use 8rd bit `0b10000000` of header to flag it.

If streaming write is enabled, which means Fury can't update written `chunk size`. In such cases, map key-value data
format will be:

```
+----------------+~~~~~~~~~~~~~~~~~+
|    KV header   |   N*2 objects   |
+----------------+~~~~~~~~~~~~~~~~~+
```

`KV header` will be header marked by `MapFieldInfo` in java. For languages such as golang, this can be computed in
advance for non-interface type mostly.

### Enum

Enum are serialized as an unsigned var int. If the order of enum values change, the deserialized enum value may not be
the value users expect. In such cases, users must register enum serializer by make it write enum value as a enumerated
string with unique hash disabled.

### Object

Object means object of `pojo/struct/bean` type.
Object will be serialized by writing its fields data in fury order.

Depends on schema compatibility, object will have different format.

#### Field order

Field will be ordered as following, every group of fields will have it's own order:

- primitive fields: larger size type first, smaller later, variable size type last.
- boxed primitive fields: same sort as primitive fields
- final fields: same type together, then sort by field name lexicographically.
- collection fields: same sort as final fields
- map fields: same sort as final fields
- other fields: same sort as final fields

#### Schema consistent

Object fields will be serialized one by one using following format:

```
Primitive field value:
+~~~~~~~~~~~~+
| value data |
+~~~~~~~~~~~~+
Boxed field value:
+-----------+~~~~~~~~~~~~~+
| null flag | field value |
+-----------+~~~~~~~~~~~~~+
field value of final type with ref tracking:
+===========+~~~~~~~~~~~~+
| ref meta  | value data |
+===========+~~~~~~~~~~~~+
field value of final type without ref tracking:
+-----------+~~~~~~~~~~~~~+
| null flag | field value |
+-----------+~~~~~~~~~~~~~+
field value of non-final type with ref tracking:
+===========+~~~~~~~~~~~~+~~~~~~~~~~~~+
| ref meta  | class meta | value data |
+===========+~~~~~~~~~~~~+~~~~~~~~~~~~+
field value of non-final type without ref tracking:
+-----------+~~~~~~~~~~~~+~~~~~~~~~~~~+
| null flag | class meta | value data |
+-----------+~~~~~~~~~~~~+~~~~~~~~~~~~+
```

#### Schema evolution
Schema evolution have similar format as schema consistent mode for object except:
- For this object type itself, `schema consistent` mode will write class by id/name, but `schema evolution` mode will write class field names, types and other meta too, see [Class meta](#class-meta).
- Class meta of `final custom type` need to be written too, because peer may not have this class defined. 

### Class

Class will be serialized using class meta format.

## Implementation guidelines

- Try to merge multiple bytes into an int/long write before writing to reduce memory IO and bound check cost.
- Read multiple bytes as an int/long, then spilt into multiple bytes to reduce memory IO and bound check cost.
- Try to use one varint/long to write flags and length together to save one byte cost and reduce memory io.
- Condition branch is less expensive compared to memory IO cost unless there are too much branches.