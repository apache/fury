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

import 'dart:typed_data';
import 'package:fury/src/deserializer_pack.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/serializer_pack.dart';
import 'package:fury/src/util/math_checker.dart';
import 'package:fury/src/config/fury_config.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';

abstract base class ArraySerializerCache extends SerializerCache{
  const ArraySerializerCache();

  @override
  Serializer getSerializer(FuryConfig conf,){
    return getSerWithRef(conf.refTracking);
  }
  Serializer getSerWithRef(bool writeRef);
}

abstract base class ArraySerializer<T> extends Serializer<List<T>> {
  const ArraySerializer(super.type, super.writeRef);
}

abstract base class NumericArraySerializer<T extends num> extends ArraySerializer<T> {
  const NumericArraySerializer(super.type, super.writeRef);

  TypedDataList<T> readToList(Uint8List copiedMem);

  int get bytesPerNum;

  @override
  TypedDataList<T> read(ByteReader br, int refId, DeserializerPack pack) {
    int num = br.readVarUint32Small7();
    return readToList(br.copyBytes(num));
  }

  @override
  void write(ByteWriter bw, covariant TypedDataList<T> v, SerializerPack pack) {
    if (!MathChecker.validInt32(v.lengthInBytes)){
      throw ArgumentError('NumArray lengthInBytes is not valid int32: ${v.lengthInBytes}');
    }
    bw.writeVarUint32(v.lengthInBytes);
    bw.writeBytes(v.buffer.asUint8List(v.offsetInBytes, v.lengthInBytes));
  }
}