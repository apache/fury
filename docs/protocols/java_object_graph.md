# Java Serialization
The data are serialized using little endian order overall.

## Basic types
### boolean
- size: 1 byte
- format: 0 for `false`, 1 for `true`

### byte
- size: 1 byte
- format: write as pure byte.

### short
- size: 2 byte
- byte order: little endian order

### char
- size: 2 byte
- byte order: little endian order

### int
- size: 1~5 byte
- positive int format: first bit in every byte indicate whether has next byte. if first bit is set i.e. `b & 0x80 == 0x80`, then next byte should be read util first bit of next byte is unset.
- Negative number will be converted to positive number by ` (v << 1) ^ (v >> 31)` to reduce cost of small negative numbers.

### long
- size: 1~9 byte
- Fury SLI(Small long as int) Encoding:
  - If long is in [-1073741824, 1073741823], encode as 4 bytes int: `| little-endian: ((int) value) << 1 |`
  - Otherwise write as 9 bytes: `| 0b1 | little-endian 8bytes long |`
- Fury PVL(Progressive Variable-length Long) Encoding:
  - positive long format: first bit in every byte indicate whether has next byte. if first bit is set i.e. `b & 0x80 == 0x80`, then next byte should be read util first bit is unset.
  - Negative number will be converted to positive number by ` (v << 1) ^ (v >> 63)` to reduce cost of small negative numbers.

### float
- size: 4 byte
- format: convert float to 4 bytes int by `Float.floatToRawIntBits`, then write as binary by little endian order.

### double
- size: 8 byte
- format: convert double to 8 bytes int by `Double.doubleToRawLongBits`, then write as binary by little endian order.

## String
Format:
- one byte for encoding: 0 for `latin`, 1 for `utf-16`, 2 for `utf-8`.
- positive varint for encoded string binary length.
- encoded string binary data based on encoding: `latin/utf-16/utf-8`.

Which encoding to choose:
- For JDK8: fury detect `latin` at runtime, if string is `latin` string, then use `latin` encoding, otherwise use `utf-16`.
- For JDK9+: fury use `coder` in `String` object for encoding, `latin`/`utf-16` will be used for encoding.
- If the string is encoded by `utf-8`, then fury will use `utf-8` to decode the data. But currently fury doesn't enable utf-8 encoding by default for java. Cross-language string serialization of fury use `utf-8` by default.

## Array

## Collection
> All collection serializer must extends `io.fury.serializer.CollectionSerializers.CollectionSerializer`.

Format:
```java
length(positive varint) | collection header | elements header | elements data
```

### collection header
- For `ArrayList/LinkedArrayList/HashSet/LinkedHashSet`, this will be empty.
- For `TreeSet`, this will be `Comparator`
- For subclass of `ArrayList`, this may be extra object field info.

### elements header
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

### elements data
Based on the elements header, the serialization of elements data may skip `ref flag`/`null flag`/`element class info`.

`io.fury.serializer.CollectionSerializers.CollectionSerializer#write/read` can be taken as an example.

## Map


## Object








