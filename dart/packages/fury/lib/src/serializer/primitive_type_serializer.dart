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

import 'package:fury/src/config/fury_config.dart';
import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/datatype/float32.dart';
import 'package:fury/src/datatype/fury_fixed_num.dart';
import 'package:fury/src/datatype/int16.dart';
import 'package:fury/src/datatype/int32.dart';
import 'package:fury/src/datatype/int8.dart';
import 'package:fury/src/deserializer_pack.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';
import 'package:fury/src/serializer_pack.dart';

abstract base class PrimitiveSerializerCache extends SerializerCache{

  const PrimitiveSerializerCache();

  @override
  Serializer getSerializer(FuryConfig conf,){
    // Currently, there are only two types of Ser for primitive types: one that write a reference
    // and one that does not, so only these two are cached here.
    bool writeRef = conf.refTracking && !conf.basicTypesRefIgnored;
    return getSerWithRef(writeRef);
  }

  Serializer getSerWithRef(bool writeRef);
}


final class _BoolSerializerCache extends PrimitiveSerializerCache{
  static BoolSerializer? serRef;
  static BoolSerializer? serNoRef;

  const _BoolSerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= BoolSerializer._(true);
      return serRef!;
    } else {
      serNoRef ??= BoolSerializer._(false);
      return serNoRef!;
    }
  }
}


final class BoolSerializer extends Serializer<bool> {
  static const SerializerCache cache = _BoolSerializerCache();
  BoolSerializer._(bool writeRef): super(ObjType.BOOL, writeRef);

  @override
  bool read(ByteReader br, int refId, DeserializerPack pack) {
    return br.readUint8() != 0;
  }

  @override
  void write(ByteWriter bw, bool v, SerializerPack pack) {
    bw.writeBool(v);
  }
}

final class _Int8SerializerCache extends PrimitiveSerializerCache{
  static Int8Serializer? serRef;
  static Int8Serializer? serNoRef;

  const _Int8SerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= Int8Serializer._(true);
      return serRef!;
    } else {
      serNoRef ??= Int8Serializer._(false);
      return serNoRef!;
    }
  }
}

// Dart does not have an int8 type. Users can specify converting a Dart int to int8 via annotations, so out-of-range errors may occur
final class Int8Serializer extends Serializer<FixedNum>{

  static const SerializerCache cache = _Int8SerializerCache();

  Int8Serializer._(bool writeRef): super(ObjType.INT8, writeRef);

  @override
  Int8 read(ByteReader br, int refId, DeserializerPack pack) {
    return Int8(br.readInt8());// Use signed 8-bit integer, which is consistent with byte in FuryJava
  }

  @override
  void write(ByteWriter bw, covariant Int8 v, SerializerPack pack) {
    // if (value < -128 || value > 127){
    //   throw FuryException.serRangeExcep(objType, value);
    // }
    bw.writeInt8(v.toInt());
  }
}

final class _Int16SerializerCache extends PrimitiveSerializerCache{
  static Int16Serializer? serRef;
  static Int16Serializer? serNoRef;

  const _Int16SerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= Int16Serializer._(true);
      return serRef!;
    } else {
      serNoRef ??= Int16Serializer._(false);
      return serNoRef!;
    }
  }
}

// Dart does not have int16. Users can specify converting Dart's int to int16 via annotation, so an out-of-range error may occur
final class Int16Serializer extends Serializer<FixedNum>{
  static const SerializerCache cache = _Int16SerializerCache();
  Int16Serializer._(bool writeRef): super(ObjType.INT16, writeRef);

  @override
  Int16 read(ByteReader br, int refId, DeserializerPack pack) {
    return Int16(br.readInt16());
  }

  @override
  void write(ByteWriter bw, covariant Int16 v, SerializerPack pack) {
    // if (value < -32768 || value > 32767){
    //   throw FuryException.serRangeExcep(objType, value);
    // }
    bw.writeInt16(v.toInt());
  }
}

final class _Int32SerializerCache extends PrimitiveSerializerCache{
  static Int32Serializer? serRef;
  static Int32Serializer? serNoRef;

  const _Int32SerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= Int32Serializer._(true);
      return serRef!;
    } else {
      serNoRef ??= Int32Serializer._(false);
      return serNoRef!;
    }
  }
}

// Dart does not have int32, users specify converting dart int to int32 through annotations, so range errors may occur
final class Int32Serializer extends Serializer<FixedNum>{
  static const SerializerCache cache = _Int32SerializerCache();
  Int32Serializer._(bool writeRef): super(ObjType.INT32, writeRef);

  @override
  Int32 read(ByteReader br, int refId, DeserializerPack pack) {
    int res = br.readVarInt32();
    return Int32(res);
  }

  @override
  void write(ByteWriter bw, covariant Int32 v, SerializerPack pack) {
    // No check is done here directly
    // if (value < -2147483648 || value > 2147483647){
    //   throw FuryException.serRangeExcep(objType, value);
    // }
    bw.writeVarInt32(v.toInt());
  }
}

final class _Int64SerializerCache extends PrimitiveSerializerCache{
  static Int64Serializer? serRef;
  static Int64Serializer? serNoRef;

  const _Int64SerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= Int64Serializer._(true);
      return serRef!;
    } else {
      serNoRef ??= Int64Serializer._(false);
      return serNoRef!;
    }
  }
}

final class Int64Serializer extends Serializer<int> {

  static const SerializerCache cache = _Int64SerializerCache();

  Int64Serializer._(bool writeRef): super(ObjType.INT64, writeRef);

  @override
  int read(ByteReader br, int refId, DeserializerPack pack) {
    return br.readVarInt64();
  }

  @override
  void write(ByteWriter bw, int v, SerializerPack pack) {
    bw.writeVarInt64(v);
  }
}

final class _Float32SerializerCache extends PrimitiveSerializerCache{
  static Float32Serializer? serRef;
  static Float32Serializer? serNoRef;

  const _Float32SerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= Float32Serializer._(true);
      return serRef!;
    } else {
      serNoRef ??= Float32Serializer._(false);
      return serNoRef!;
    }
  }
}

// Dart does not have float32; the user can specify converting Dart double to float32 through annotation, so precision errors may occur
final class Float32Serializer extends Serializer<FixedNum>{

  static const SerializerCache cache = _Float32SerializerCache();

  Float32Serializer._(bool writeRef): super(ObjType.FLOAT32, writeRef);

  @override
  Float32 read(ByteReader br, int refId, DeserializerPack pack) {
    return Float32(br.readFloat32());
  }

  @override
  void write(ByteWriter bw, covariant Float32 v, SerializerPack pack) {
    // No checks are performed here
    // if (value.isInfinite || value.isNaN || value < -3.4028235e38 || value > 3.4028235e38){
    //   throw FuryException.serRangeExcep(objType, value);
    // }
    bw.writeFloat32(v.toDouble());
  }
}

final class _Float64SerializerCache extends PrimitiveSerializerCache{
  static Float64Serializer? serRef;
  static Float64Serializer? serNoRef;

  const _Float64SerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= Float64Serializer._(true);
      return serRef!;
    } else {
      serNoRef ??= Float64Serializer._(false);
      return serNoRef!;
    }
  }
}

final class Float64Serializer extends Serializer<double>{

  static const SerializerCache cache = _Float64SerializerCache();

  Float64Serializer._(bool writeRef): super(ObjType.FLOAT64, writeRef);

  @override
  double read(ByteReader br, int refId, DeserializerPack pack) {
    return br.readFloat64();
  }

  @override
  void write(ByteWriter bw, double v, SerializerPack pack) {
    bw.writeFloat64(v);
  }
}
