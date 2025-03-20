import 'dart:typed_data';

import 'package:fury_core/src/collection/stack.dart';
import 'package:fury_core/src/fury_data_type/float32.dart';
import 'package:fury_core/src/fury_data_type/int8.dart';
import 'package:fury_core/src/meta/class_info.dart';
import 'package:fury_core/src/meta/spec_wraps/type_spec_wrap.dart';
import 'package:fury_core/src/resolver/ref/deser_ref_resolver.dart';
import 'package:fury_core/src/resolver/struct_hash_resolver.dart';
import 'package:fury_core/src/resolver/xtype_resolver.dart';
import 'package:fury_core/src/ser/fury_header_ser.dart';
import 'package:fury_core/src/deser_pack.dart';

import 'config/fury_config.dart';
import 'const/obj_type.dart';
import 'const/ref_flag.dart';
import 'fury_data_type/int16.dart';
import 'fury_data_type/int32.dart';
import 'memory/byte_reader.dart';
import 'ser/ser.dart' show Ser;

class FuryDeserDirector {
  // singleton
  static final FuryDeserDirector _instance = FuryDeserDirector._internal();
  static FuryDeserDirector get I => _instance;
  FuryDeserDirector._internal();
  // static part
  static final FuryHeaderSer _furyHeaderSer = FuryHeaderSer.I;

  Object? deser(Uint8List bytes, FuryConfig conf, XtypeResolver xtypeResolver, [ByteReader? reader]) {
    var br = reader ?? ByteReader.forBytes(bytes,);
    HeaderBrief? header = _furyHeaderSer.read(br, conf);
    if (header == null) return null;

    DeserPack deserPack = DeserPack(
      StructHashResolver.inst,
      xtypeResolver.getTagByCustomDartType,
      header,
      this,
      DeserRefResolver.getOne(conf.refTracking),
      xtypeResolver,
      Stack<TypeSpecWrap>(),
    );
    return xReadRefNoSer(br, deserPack);
  }

  Object? xReadRefNoSer(ByteReader br, DeserPack pack) {
    int refFlag = br.readInt8();
    //assert(RefFlag.checkAllow(refFlag));
    //assert(refFlag >= RefFlag.NULL.id);
    if (refFlag == RefFlag.NULL.id) return null;
    DeserRefResolver refResolver = pack.refResolver;
    if (refFlag == RefFlag.TRACK_ALREADY.id){
      int refId = br.readVarUint32Small14();
      return refResolver.getObj(refId);
    }
    if (refFlag >= RefFlag.UNTRACK_NOT_NULL.id){
      // 说明是一定要重新解析的
      ClassInfo clsInfo = pack.xtypeResolver.readClassInfo(br);
      int refId = refResolver.reserveId();
      Object o = _xDeser(br, clsInfo, refId, pack);
      refResolver.setRef(refId, o);
      return o;
    }
    assert(false);
    return null; // wont reach here
  }

  Object? xReadRefWithSer(ByteReader br, Ser ser, DeserPack pack) {
    if (ser.writeRef){
      DeserRefResolver refResolver = pack.refResolver;
      int refFlag = br.readInt8();
      //assert(RefFlag.checkAllow(refFlag));
      //assert(refFlag >= RefFlag.NULL.id);
      if (refFlag == RefFlag.NULL.id) return null;
      if (refFlag == RefFlag.TRACK_ALREADY.id){
        int refId = br.readVarUint32Small14();
        return refResolver.getObj(refId);
      }
      if (refFlag >= RefFlag.UNTRACK_NOT_NULL.id){
        // 说明是一定要重新解析的
        int refId = refResolver.reserveId();
        Object o = ser.read(br, refId, pack);
        refResolver.setRef(refId, o);
        return o;
      }
    }
    int headFlag = br.readInt8();
    if (headFlag == RefFlag.NULL.id) return null;
    return ser.read(br, -1, pack);
  }

  /// this method will only be invoked by Fury::_xReadRef
  Object _xDeser(ByteReader br, ClassInfo clsInfo, int refId, DeserPack pack) {
    switch (clsInfo.objType) {
      case ObjType.BOOL:
        return br.readInt8() != 0;
      case ObjType.INT8:
        return Int8(br.readInt8());
      case ObjType.INT16:
        return Int16(br.readInt16());
      case ObjType.INT32:
      case ObjType.VAR_INT32:
        return Int32(br.readVarInt32());
      case ObjType.INT64:
      case ObjType.VAR_INT64:
      case ObjType.SLI_INT64:
        return br.readVarInt64();
      case ObjType.FLOAT32:
        return Float32(br.readFloat32());
      case ObjType.FLOAT64:
        return br.readFloat64();
      default:
        Object o = clsInfo.ser.read(br, refId, pack);
        return o;
    }
  }
}