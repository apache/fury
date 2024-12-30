/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#include <cstdint> // For fixed-width integer types

namespace fury {
enum class TypeId : int32_t {
  // a boolean value (true or false).
  BOOL = 1,
  // a 8-bit signed integer.
  INT8 = 2,
  // a 16-bit signed integer.
  INT16 = 3,
  // a 32-bit signed integer.
  INT32 = 4,
  // a 32-bit signed integer which use fury var_int32 encoding.
  VAR_INT32 = 5,
  // a 64-bit signed integer.
  INT64 = 6,
  // a 64-bit signed integer which use fury PVL encoding.
  VAR_INT64 = 7,
  // a 64-bit signed integer which use fury SLI encoding.
  SLI_INT64 = 8,
  // a 16-bit floating point number.
  FLOAT16 = 9,
  // a 32-bit floating point number.
  FLOAT32 = 10,
  // a 64-bit floating point number including NaN and Infinity.
  FLOAT64 = 11,
  // a text string encoded using Latin1/UTF16/UTF-8 encoding.
  STRING = 12,
  // a data type consisting of a set of named values. Rust enum with
  // non-predefined field values are not supported as an enum
  ENUM = 13,
  // an enum whose value will be serialized as the registered name.
  NAMED_ENUM = 14,
  // a morphic(final) type serialized by Fury Struct serializer. i.e. it doesn't
  // have subclasses. Suppose we're
  // deserializing `List<SomeClass>`, we can save dynamic serializer dispatch
  // since `SomeClass` is morphic(final).
  STRUCT = 15,
  // a type which is not morphic(not final). i.e. it has subclasses. Suppose
  // we're deserializing
  // `List<SomeClass>`, we must dispatch serializer dynamically since
  // `SomeClass` is polymorphic(non-final).
  POLYMORPHIC_STRUCT = 16,
  // a morphic(final) type serialized by Fury compatible Struct serializer.
  COMPATIBLE_STRUCT = 17,
  // a non-morphic(non-final) type serialized by Fury compatible Struct
  // serializer.
  POLYMORPHIC_COMPATIBLE_STRUCT = 18,
  // a `struct` whose type mapping will be encoded as a name.
  NAMED_STRUCT = 19,
  // a `polymorphic_struct` whose type mapping will be encoded as a name.
  NAMED_POLYMORPHIC_STRUCT = 20,
  // a `compatible_struct` whose type mapping will be encoded as a name.
  NAMED_COMPATIBLE_STRUCT = 21,
  // a `polymorphic_compatible_struct` whose type mapping will be encoded as a
  // name.
  NAMED_POLYMORPHIC_COMPATIBLE_STRUCT = 22,
  // a type which will be serialized by a customized serializer.
  EXT = 23,
  // an `ext` type which is not morphic(not final).
  POLYMORPHIC_EXT = 24,
  // an `ext` type whose type mapping will be encoded as a name.
  NAMED_EXT = 25,
  // an `polymorphic_ext` type whose type mapping will be encoded as a name.
  NAMED_POLYMORPHIC_EXT = 26,
  // a sequence of objects.
  LIST = 27,
  // an unordered set of unique elements.
  SET = 28,
  // a map of key-value pairs. Mutable types such as
  // `list/map/set/array/tensor/arrow` are not allowed as key of map.
  MAP = 29,
  // an absolute length of time, independent of any calendar/timezone, as a
  // count of nanoseconds.
  DURATION = 30,
  // a point in time, independent of any calendar/timezone, as a count of
  // nanoseconds. The count is relative
  // to an epoch at UTC midnight on January 1, 1970.
  TIMESTAMP = 31,
  // a naive date without timezone. The count is days relative to an epoch at
  // UTC midnight on Jan 1, 1970.
  LOCAL_DATE = 32,
  // exact decimal value represented as an integer value in two's complement.
  DECIMAL = 33,
  // an variable-length array of bytes.
  BINARY = 34,
  // a multidimensional array which every sub-array can have different sizes but
  // all have same type.
  // only allow numeric components. Other arrays will be taken as List. The
  // implementation should support the
  // interoperability between array and list.
  ARRAY = 35,
  // one dimensional bool array.
  BOOL_ARRAY = 36,
  // one dimensional int16 array.
  INT8_ARRAY = 37,
  // one dimensional int16 array.
  INT16_ARRAY = 38,
  // one dimensional int32 array.
  INT32_ARRAY = 39,
  // one dimensional int64 array.
  INT64_ARRAY = 40,
  // one dimensional half_float_16 array.
  FLOAT16_ARRAY = 41,
  // one dimensional float32 array.
  FLOAT32_ARRAY = 42,
  // one dimensional float64 array.
  FLOAT64_ARRAY = 43,
  // an arrow [record
  // batch](https://arrow.apache.org/docs/cpp/tables.html#record-batches)
  // object.
  ARROW_RECORD_BATCH = 44,
  // an arrow [table](https://arrow.apache.org/docs/cpp/tables.html#tables)
  // object.
  ARROW_TABLE = 45,
  BOUND = 64
};

inline bool IsNamespacedType(int32_t type_id) {
  switch (static_cast<TypeId>(type_id)) {
  case TypeId::NAMED_ENUM:
  case TypeId::NAMED_STRUCT:
  case TypeId::NAMED_POLYMORPHIC_STRUCT:
  case TypeId::NAMED_COMPATIBLE_STRUCT:
  case TypeId::NAMED_POLYMORPHIC_COMPATIBLE_STRUCT:
  case TypeId::NAMED_EXT:
  case TypeId::NAMED_POLYMORPHIC_EXT:
    return true;
  default:
    return false;
  }
}

} // namespace fury
