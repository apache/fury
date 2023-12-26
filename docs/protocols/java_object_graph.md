# Fury Java Serialization Specification

## Spec overview

The data are serialized using little endian order overall. If bytes swap is costly, the byte order will be encoded as a
flag in data.

The overall format are:

```
| fury header | object ref meta | object class meta | object value data |
```

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
- If class is not registered, fury will write one byte `0b1` first, the little bit is different first bit of encoded
  class id, which is `0`. Fury can use this information to determine whether read class by class id.
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
> TODO: We have plan to streamline meta writing but not started yet.

### Schema consistent


### Schema evolution


## Enumerated String

Enumerated string are mainly used to encode class name and field names. The format consists of header and binary.

Header are written using little endian order, Fury can read this flag first to determine how to deserialize the data.

### Header

#### Write by data

If string hasn't been written before, the data will be written as follows:

```
| unsigned int: string binary size + 1bit: not written before | 61bits: murmur hash + 3 bits encoding flags | string binary |
```

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

- one byte for encoding: 0 for `latin`, 1 for `utf-16`, 2 for `utf-8`.
- positive varint for encoded string binary length.
- encoded string binary data based on encoding: `latin/utf-16/utf-8`.

Which encoding to choose:

- For JDK8: fury detect `latin` at runtime, if string is `latin` string, then use `latin` encoding, otherwise
  use `utf-16`.
- For JDK9+: fury use `coder` in `String` object for encoding, `latin`/`utf-16` will be used for encoding.
- If the string is encoded by `utf-8`, then fury will use `utf-8` to decode the data. But currently fury doesn't enable
  utf-8 encoding by default for java. Cross-language string serialization of fury use `utf-8` by default.

### Collection

> All collection serializer must extends `io.fury.serializer.collection.CollectionSerializer`.

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

- Whether track elements ref, use first bit `0b1` of header to flag it.
- Whether collection has null, use second bit `0b10` of header to flag it. If ref tracking is enabled for this
  element type, this flag is invalid.
- Whether collection elements type is not declare type, use 3rd bit `0b100` of header to flag it.
- Whether collection elements type different, use 4rd bit `0b1000` of header to flag it.

By default, all bits are unset, which means all elements won't track ref, all elements are same type,, not null and the
actual element is the declare type in custom class field.

#### Elements data

Based on the elements header, the serialization of elements data may skip `ref flag`/`null flag`/`element class info`.

`io.fury.serializer.collection.CollectionSerializer#write/read` can be taken as an example.

### Array

#### Primitive array

#### Object array

### Map

### Enum

Enum are serialized as an

### Object

### Class

## Implementation guidelines

- Try to merge multiple bytes into an int/long write before writing to reduce memory IO and bound check cost.
- Read multiple bytes as an int/long, then spilt into multiple bytes to reduce memory IO and bound check cost.
- Try to use one varint/long to write flags and length together to save one byte cost and reduce memory io.
- Condition branch is less expensive compared to memory IO cost unless there are too much branches.