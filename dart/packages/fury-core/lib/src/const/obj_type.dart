// TODO: not all types are supported, furyJava也一样，例如Uint8,和Uint16, Type枚举有，并且也设计了如此的serializer, 但是确实没有用到
/// TODO: furyJava 对这些type是否进行writeRef 管理更加细致，例如对于时间类型的控制， 对于字符串的控制，而不仅仅是对于basicType的控制,这里先只进行对于basicType的控制

enum ObjType {
  /// A NULL type having no physical storage
  UNKNOWN_YET(0, false), // 这个值，不是没有意义的，例如某个字段是父类/非具体类，无法再静态代码生成时分析出来

  // x
  /// Boolean as 1 bit, LSB bit-packed ordering
  BOOL(1, true), // 1 // 这里的字符串的意思是，dart类型当是bool时，默认使用ObjType.BOOL这个类型

  // x
  /// Signed 8-bit little-endian integer
  INT8(2, true), // 2

  // x
  /// Signed 16-bit little-endian integer
  INT16(3, true), // 3

  // x
  /// Signed 32-bit little-endian integer
  INT32(4, true), // 4

  // x
  /// var_int32: a 32-bit signed integer which uses fury var_int32 encoding.
  VAR_INT32(5, true), // 5

  // x
  /// Signed 64-bit little-endian integer
  INT64(6, true), // 6

  // x
  /// var_int64: a 64-bit signed integer which uses fury var_int64 encoding.
  VAR_INT64(7, true), // 7

  // x
  /// sli_int64: a 64-bit signed integer which uses fury SLI encoding.
  SLI_INT64(8, true), // 8

  /// float16: a 16-bit floating point number.
  FLOAT16(9, true), // 9

  // x
  /// float32: a 32-bit floating point number.
  FLOAT32(10, true), // 10

  // x
  /// float64: a 64-bit floating point number including NaN and Infinity.
  FLOAT64(11, true), // 11

  // x
  /// string: a text string encoded using Latin1/UTF16/UTF-8 encoding.
  STRING(12, true), // 12

  // x
  /// enum: a data type consisting of a set of named values.
  ENUM(13, true), // 13

  /// named_enum: an enum whose value will be serialized as the registered name.
  NAMED_ENUM(14, true), // 14

  /// A morphic(final) type serialized by Fury Struct serializer. i.e. it doesn't have subclasses.
  /// Suppose we're deserializing {@code List<SomeClass>}, we can save dynamic serializer dispatch
  /// since `SomeClass` is morphic(final).
  STRUCT(15, false), // 15

  /// A morphic(final) type serialized by Fury compatible Struct serializer.
  COMPATIBLE_STRUCT(16, false), // 16

  // x
  /// A `struct` whose type mapping will be encoded as a name.
  NAMED_STRUCT(17, false), // 17

  /// A `compatible_struct` whose type mapping will be encoded as a name.
  NAMED_COMPATIBLE_STRUCT(18, false), // 18

  /// A type which will be serialized by a customized serializer.
  EXT(19, false), // 19

  /// An `ext` type whose type mapping will be encoded as a name.
  NAMED_EXT(20, false), // 20

  // x
  /// A sequence of objects.
  LIST(21, false), // 21

  // x
  /// An unordered set of unique elements.
  SET(22, false), // 22

  // x
  /// A map of key-value pairs. Mutable types such as `list/map/set/array/tensor/arrow` are not
  /// allowed as key of map.
  MAP(23, false), // 23

  /// An absolute length of time, independent of any calendar/timezone, as a count of nanoseconds.
  DURATION(24, true), // 24

  // TODO: here time
  // x
  /// A point in time, independent of any calendar/timezone, as a count of nanoseconds. The count is
  /// relative to an epoch at UTC midnight on January 1, 1970.
  TIMESTAMP(25, true), // 25

  // TODO: here time
  /// A naive date without timezone. The count is days relative to an epoch at UTC midnight on Jan 1,
  /// 1970.
  LOCAL_DATE(26, true), // 26

  /// Exact decimal value represented as an integer value in two's complement.
  DECIMAL(27, true), // 27

  // x
  /// A variable-length array of bytes.
  BINARY(28, true), // 28

  /// x
  /// A multidimensional array where every sub-array can have different sizes but all have the same
  /// type. Only numeric components allowed. Other arrays will be taken as List. The implementation
  /// should support interoperability between array and list.
  ARRAY(29, false), // 29

  /// One dimensional bool array.
  BOOL_ARRAY(30, true), // 30

  /// One dimensional int8 array.
  INT8_ARRAY(31, true), // 31

  /// One dimensional int16 array.
  INT16_ARRAY(32, true),
  
  /// One dimensional int32 array.
  INT32_ARRAY(33, true),
  
  /// One dimensional int64 array.
  INT64_ARRAY(34, true),
  
  /// One dimensional half_float_16 array.
  FLOAT16_ARRAY(35, true),
  
  /// One dimensional float32 array.
  FLOAT32_ARRAY(36, true),
  
  /// One dimensional float64 array.
  FLOAT64_ARRAY(37, true),
  
  /// An (arrow record batch) object.
  ARROW_RECORD_BATCH(38, false),
  
  /// An (arrow table) object.
  ARROW_TABLE(39, false);


  final int id;
  final bool independent;

  const ObjType(this.id,this.independent,);

  static ObjType? fromId(int id){
    // 目前的实现是线性的，所以这里更简单, 如果之后id与ordinal并不规则了，就不能这样了
    if (id >= 1 && id <= 39) return ObjType.values[id];
    return null;
  }

  // Helper methods
  bool isStructType() {
    return this == STRUCT
        || this == COMPATIBLE_STRUCT
        || this == NAMED_STRUCT
        || this == NAMED_COMPATIBLE_STRUCT;
  }
  
  bool isTimeType() {
    return this == TIMESTAMP
        || this == LOCAL_DATE
        || this == DURATION;
  }
  
  
}