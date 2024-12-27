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

package org.apache.fury.type;

public class Types {

  /** bool: a boolean value (true or false). */
  public static final int BOOL = 1;

  /** int8: a 8-bit signed integer. */
  public static final int INT8 = 2;

  /** int16: a 16-bit signed integer. */
  public static final int INT16 = 3;

  /** int32: a 32-bit signed integer. */
  public static final int INT32 = 4;

  /** var_int32: a 32-bit signed integer which use fury var_int32 encoding. */
  public static final int VAR_INT32 = 5;

  /** int64: a 64-bit signed integer. */
  public static final int INT64 = 6;

  /** var_int64: a 64-bit signed integer which use fury PVL encoding. */
  public static final int VAR_INT64 = 7;

  /** sli_int64: a 64-bit signed integer which use fury SLI encoding. */
  public static final int SLI_INT64 = 8;

  /** float16: a 16-bit floating point number. */
  public static final int FLOAT16 = 9;

  /** float32: a 32-bit floating point number. */
  public static final int FLOAT32 = 10;

  /** float64: a 64-bit floating point number including NaN and Infinity. */
  public static final int FLOAT64 = 11;

  /** string: a text string encoded using Latin1/UTF16/UTF-8 encoding. */
  public static final int STRING = 12;

  /**
   * enum: a data type consisting of a set of named values. Rust enum with non-predefined field
   * values are not \ supported as an enum.
   */
  public static final int ENUM = 13;

  /** named_enum: an enum whose value will be serialized as the registered name. */
  public static final int NAMED_ENUM = 14;

  /**
   * a morphic(final) type serialized by Fury Struct serializer. i.e. it doesn't have subclasses.
   * Suppose we're deserializing {@code List<SomeClass>}`, we can save dynamic serializer dispatch
   * since `SomeClass` is morphic(final).
   */
  public static final int STRUCT = 15;

  /**
   * a type which is polymorphic(not final). i.e. it has subclasses. Suppose we're deserializing
   * {@code List<SomeClass>}`, we must dispatch serializer dynamically since `SomeClass` is
   * polymorphic(non-final).
   */
  public static final int POLYMORPHIC_STRUCT = 16;

  /** a morphic(final) type serialized by Fury compatible Struct serializer. */
  public static final int COMPATIBLE_STRUCT = 17;

  /** a non-morphic(non-final) type serialized by Fury compatible Struct serializer. */
  public static final int POLYMORPHIC_COMPATIBLE_STRUCT = 18;

  /** a `struct` whose type mapping will be encoded as a name. */
  public static final int NAMED_STRUCT = 19;

  /** a `polymorphic_struct` whose type mapping will be encoded as a name. */
  public static final int NAMED_POLYMORPHIC_STRUCT = 20;

  /** a `compatible_struct` whose type mapping will be encoded as a name. */
  public static final int NAMED_COMPATIBLE_STRUCT = 21;

  /** a `polymorphic_compatible_struct` whose type mapping will be encoded as a name. */
  public static final int NAMED_POLYMORPHIC_COMPATIBLE_STRUCT = 22;

  /** a type which will be serialized by a customized serializer. */
  public static final int EXT = 23;

  /** an `ext` type which is not morphic(not final). */
  public static final int POLYMORPHIC_EXT = 24;

  /** an `ext` type whose type mapping will be encoded as a name. */
  public static final int NAMED_EXT = 25;

  /** an `polymorphic_ext` type whose type mapping will be encoded as a name. */
  public static final int NAMED_POLYMORPHIC_EXT = 26;

  /** a sequence of objects. */
  public static final int LIST = 27;

  /** an unordered set of unique elements. */
  public static final int SET = 28;

  /**
   * a map of key-value pairs. Mutable types such as `list/map/set/array/tensor/arrow` are not
   * allowed as key of map.
   */
  public static final int MAP = 29;

  /**
   * an absolute length of time, independent of any calendar/timezone, as a count of nanoseconds.
   */
  public static final int DURATION = 30;

  /**
   * timestamp: a point in time, independent of any calendar/timezone, as a count of nanoseconds.
   * The count is relative to an epoch at UTC midnight on January 1, 1970.
   */
  public static final int TIMESTAMP = 31;

  /**
   * a naive date without timezone. The count is days relative to an epoch at UTC midnight on Jan 1,
   * 1970.
   */
  public static final int LOCAL_DATE = 32;

  /** exact decimal value represented as an integer value in two's complement. */
  public static final int DECIMAL = 33;

  /** an variable-length array of bytes. */
  public static final int BINARY = 34;

  /**
   * a multidimensional array which every sub-array can have different sizes but all have same type.
   * only allow numeric components. Other arrays will be taken as List. The implementation should
   * support the interoperability between array and list.
   */
  public static final int ARRAY = 35;

  /** one dimensional int16 array. */
  public static final int BOOL_ARRAY = 36;

  /** one dimensional int8 array. */
  public static final int INT8_ARRAY = 37;

  /** one dimensional int16 array. */
  public static final int INT16_ARRAY = 38;

  /** one dimensional int32 array. */
  public static final int INT32_ARRAY = 39;

  /** one dimensional int64 array. */
  public static final int INT64_ARRAY = 40;

  /** one dimensional half_float_16 array. */
  public static final int FLOAT16_ARRAY = 41;

  /** one dimensional float32 array. */
  public static final int FLOAT32_ARRAY = 42;

  /** one dimensional float64 array. */
  public static final int FLOAT64_ARRAY = 43;

  /**
   * an (<a href="https://arrow.apache.org/docs/cpp/tables.html#record-batches">arrow record
   * batch</a>) object.
   */
  public static final int ARROW_RECORD_BATCH = 44;

  /** an (<a href="https://arrow.apache.org/docs/cpp/tables.html#tables">arrow table</a>) object. */
  public static final int ARROW_TABLE = 45;

  public static boolean isStructType(int value) {
    return value == STRUCT
        || value == POLYMORPHIC_STRUCT
        || value == COMPATIBLE_STRUCT
        || value == POLYMORPHIC_COMPATIBLE_STRUCT
        || value == NAMED_STRUCT
        || value == NAMED_POLYMORPHIC_STRUCT
        || value == NAMED_COMPATIBLE_STRUCT
        || value == NAMED_POLYMORPHIC_COMPATIBLE_STRUCT;
  }
}
