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

import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/datatype/timestamp.dart';
import 'package:fury/src/deserializer_pack.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer/serializer_cache.dart';
import 'package:fury/src/serializer/time/time_serializer_cache.dart';
import 'package:fury/src/serializer_pack.dart';

final class _TimestampSerializerCache extends TimeSerializerCache{
  static TimestampSerializer? serRef;
  static TimestampSerializer? serNoRef;

  const _TimestampSerializerCache();

  @override
  Serializer getSerWithRef(bool writeRef) {
    if (writeRef){
      serRef ??= TimestampSerializer._(true);
      return serRef!;
    } else {
      serNoRef ??= TimestampSerializer._(false);
      return serNoRef!;
    }
  }
}

final class TimestampSerializer extends Serializer<TimeStamp> {

  static const SerializerCache cache = _TimestampSerializerCache();

  TimestampSerializer._(bool writeRef) : super(ObjType.TIMESTAMP, writeRef);

  @override
  TimeStamp read(ByteReader br, int refId, DeserializerPack pack) {
    int microseconds = br.readInt64();
    // attention: UTC
    return TimeStamp(microseconds);
  }

  @override
  void write(ByteWriter bw, TimeStamp v, SerializerPack pack) {
    bw.writeInt64(v.microsecondsSinceEpoch);
  }
}