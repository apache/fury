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
import 'package:fury/src/collection/stack.dart';
import 'package:fury/src/config/fury_config.dart';
import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/const/ref_flag.dart';
import 'package:fury/src/memory/byte_writer.dart';
import 'package:fury/src/meta/type_info.dart';
import 'package:fury/src/meta/spec_wraps/type_spec_wrap.dart';
import 'package:fury/src/resolver/meta_string_writing_resolver.dart';
import 'package:fury/src/resolver/serialization_ref_resolver.dart';
import 'package:fury/src/resolver/struct_hash_resolver.dart';
import 'package:fury/src/resolver/xtype_resolver.dart';
import 'package:fury/src/serializer/fury_header_serializer.dart';
import 'package:fury/src/serializer/serializer.dart';
import 'package:fury/src/serializer_pack.dart';
import 'package:fury/src/datatype/int8.dart';
import 'package:fury/src/datatype/int16.dart';
import 'package:fury/src/datatype/int32.dart';
import 'package:fury/src/datatype/float32.dart';

class SerializeCoordinator {
  static final SerializeCoordinator _instance = SerializeCoordinator._internal();
  static SerializeCoordinator get I => _instance;
  SerializeCoordinator._internal();

  static final FuryHeaderSerializer _furyHeaderSer = FuryHeaderSerializer.I;

  void _write(Object? obj, FuryConfig conf, XtypeResolver xtypeResolver, ByteWriter writer) {
    _furyHeaderSer.write(writer, obj == null, conf);
    SerializerPack pack = SerializerPack(
      StructHashResolver.inst,
      xtypeResolver.getTagByCustomDartType,
      this,
      xtypeResolver,
      SerializationRefResolver.getOne(conf.refTracking),
      SerializationRefResolver.noRefResolver,
      MetaStringWritingResolver.newInst,
      Stack<TypeSpecWrap>(),
    );
    xWriteRefNoSer(writer, obj, pack);
    // pack.resetAndRecycle();
  }

  Uint8List write(Object? obj, FuryConfig conf, XtypeResolver xtypeResolver,) {
    ByteWriter bw = ByteWriter();
    _write(obj, conf, xtypeResolver, bw);
    return bw.takeBytes();
  }

  void writeWithWriter(Object? obj, FuryConfig conf, XtypeResolver xtypeResolver, ByteWriter writer) {
    _write(obj, conf, xtypeResolver, writer);
  }

  void xWriteRefNoSer(ByteWriter bw, Object? obj, SerializerPack pack) {
    SerializationRefMeta serRef = pack.refResolver.getRefId(obj);
    bw.writeInt8(serRef.refFlag.id);
    if (serRef.refId != null) {
      bw.writeVarUint32(serRef.refId!);
    }
    if (serRef.refFlag.noNeedToSer) return;
    TypeInfo typeInfo = pack.xtypeResolver.writeGetTypeInfo(bw, obj!, pack);
    switch (typeInfo.objType) {
      case ObjType.BOOL:
        bw.writeBool(obj as bool);
        break;
      case ObjType.INT8:
        bw.writeInt8((obj as Int8).value);
        break;
      case ObjType.INT16:
        bw.writeInt16((obj as Int16).value);
        break;
      case ObjType.INT32:
      case ObjType.VAR_INT32:
        bw.writeVarInt32((obj as Int32).value);
        break;
      case ObjType.INT64:
      case ObjType.VAR_INT64:
        bw.writeVarInt64(obj as int);
        break;
      case ObjType.FLOAT32:
        bw.writeFloat32((obj as Float32).value);
        break;
      case ObjType.FLOAT64:
        bw.writeFloat64(obj as double);
        break;
      default:
        typeInfo.ser.write(bw, obj, pack);
    }
  }

  void xWriteRefWithSer(ByteWriter bw, Serializer ser, Object? obj, SerializerPack pack) {
    if (ser.writeRef) {
      SerializationRefMeta serRef = pack.refResolver.getRefId(obj);
      bw.writeInt8(serRef.refFlag.id);
      if (serRef.refId != null) {
        bw.writeVarUint32(serRef.refId!);
      }
      if (serRef.refFlag.noNeedToSer) return;
      ser.write(bw, obj, pack);
    }else{
      RefFlag refFlag = pack.noRefResolver.getRefFlag(obj);
      bw.writeInt8(refFlag.id);
      if (refFlag.noNeedToSer) return;
      ser.write(bw, obj, pack);
    }
  }
}