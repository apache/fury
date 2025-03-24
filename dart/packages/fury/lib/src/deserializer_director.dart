import 'dart:typed_data';
import 'package:fury/src/collection/stack.dart';
import 'package:fury/src/config/fury_config.dart';
import 'package:fury/src/const/obj_type.dart';
import 'package:fury/src/const/ref_flag.dart';
import 'package:fury/src/datatype/float32.dart';
import 'package:fury/src/datatype/int16.dart';
import 'package:fury/src/datatype/int32.dart';
import 'package:fury/src/datatype/int8.dart';
import 'package:fury/src/memory/byte_reader.dart';
import 'package:fury/src/meta/type_info.dart';
import 'package:fury/src/meta/spec_wraps/type_spec_wrap.dart';
import 'package:fury/src/resolver/ref/deser_ref_resolver.dart';
import 'package:fury/src/resolver/struct_hash_resolver.dart';
import 'package:fury/src/resolver/xtype_resolver.dart';
import 'package:fury/src/serializer/fury_header_serializer.dart';
import 'package:fury/src/deserializer_pack.dart';
import 'package:fury/src/serializer/serializer.dart';

class DeserializeDirector {

  static final DeserializeDirector _instance = DeserializeDirector._internal();
  static DeserializeDirector get I => _instance;
  DeserializeDirector._internal();

  static final FuryHeaderSerializer _furyHeaderSer = FuryHeaderSerializer.I;

  Object? deser(Uint8List bytes, FuryConfig conf, XtypeResolver xtypeResolver, [ByteReader? reader]) {
    var br = reader ?? ByteReader.forBytes(bytes,);
    HeaderBrief? header = _furyHeaderSer.read(br, conf);
    if (header == null) return null;

    DeserializerPack deserPack = DeserializerPack(
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

  Object? xReadRefNoSer(ByteReader br, DeserializerPack pack) {
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
      // must deserialize
      TypeInfo typeInfo = pack.xtypeResolver.readTypeInfo(br);
      int refId = refResolver.reserveId();
      Object o = _xDeser(br, typeInfo, refId, pack);
      refResolver.setRef(refId, o);
      return o;
    }
    assert(false);
    return null; // wont reach here
  }

  Object? xReadRefWithSer(ByteReader br, Serializer ser, DeserializerPack pack) {
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
        // must deserialize
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
  Object _xDeser(ByteReader br, TypeInfo typeInfo, int refId, DeserializerPack pack) {
    switch (typeInfo.objType) {
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
        Object o = typeInfo.ser.read(br, refId, pack);
        return o;
    }
  }
}