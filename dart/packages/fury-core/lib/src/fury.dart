// FuryClassSpec _getClassByTag(ByteReader reader, MetaStringResolver msResolver){
//   String tag = msResolver.readMetaString(reader);
//   FuryClassSpec spec = _ctx.getClassSpecByTag(tag);
//   return spec;
// }


// Object? _xNativeObjDeser(ByteReader br, DeserConfig deserConf) {
//   // TODO: native object deserialization are not implemented yet.
//   int nativeObjStartOffset = br.readInt32();
//   int nativeObjSize = br.readInt32();
//   // TODO: just skip this process
//   return _xReadRef(br, deserConf);
// }

// Object _xDeserObj(ByteReader br, DeserConfig deserConf, [Ser? ser]) {
//   int typeId = br.readInt16();
//   if (typeId == DeserFlags.notSupportXLangFlag){
//     // TODO: 这里的异常需要更细致
//     throw Exception("not support xlang flag");
//   }
//   if (typeId <= DeserFlags.notSupportXLangFlag){
//     // no method
//     throw Exception("not support xlang flag");
//   }
//   // now all the typeId is > 0
//   if (typeId == ObjType.FURY_TYPE_TAG.id){
//     FuryClassSpec spec = _getClassByTag(br, deserConf.msResolver);
//     Object obj = _xDeserStrcut(br, spec);
//     return obj;
//   }else{
//     // 说明是内部注册类型
//     // TODO: 之类先不进行范围检查，之后若找不到对应的ser, 自然会报错
//     Ser ser = SerPool.I(typeId);
//     return ser.read(br, deserConf);
//   }
// }

// FuryClassSpec _getClassByTag(ByteReader br, MetaStringResolver msResolver){
//   String tag = msResolver.readMetaString(br);
//   FuryClassSpec spec = _ctx.getClassSpecByTag(tag);
//   return spec;
// }

import 'dart:typed_data';

import 'package:fury_core/src/base_fury.dart';
import 'package:fury_core/src/memory/byte_reader.dart';
import 'package:fury_core/src/memory/byte_writer.dart';
import 'package:fury_core/src/code_gen/entity/struct_hash_pair.dart';
import 'package:fury_core/src/dev_annotation/optimize.dart';
import 'package:fury_core/src/fury_deser_director.dart';
import 'package:fury_core/src/fury_ser_director.dart';
import 'package:fury_core/src/manager/fury_config_manager.dart';
import 'package:fury_core/src/resolver/xtype_resolver.dart';

import 'config/fury_config.dart';
import 'meta/specs/custom_type_spec.dart';
import 'ser/ser.dart' show Ser;

final class Fury implements BaseFury{

  // static part
  static final FuryDeserDirector _deserDirector = FuryDeserDirector.I;
  static final FurySerDirector _serDirector = FurySerDirector.I;

  final FuryConfig _conf;
  late final XtypeResolver _xtypeResolver;

  Fury({
    required bool xlangMode,
    bool isLittleEndian = true,
    bool refTracking = true,
    bool basicTypesRefIgnored = true,
    bool timeRefIgnored = true,
    // bool stringRefIgnored = true,
  }) : _conf = FuryConfigManager.inst.createConfig(
    xlangMode: xlangMode,
    isLittleEndian: isLittleEndian,
    refTracking: refTracking,
    basicTypesRefIgnored: basicTypesRefIgnored,
    timeRefIgnored: timeRefIgnored,
    // stringRefIgnored: stringRefIgnored,
  ){
    _xtypeResolver = XtypeResolver.newOne(_conf);
  }

  @override
  @inline
  void register(CustomTypeSpec spec, [String? tag]) {
    _xtypeResolver.reg(spec, tag);
  }

  @inline
  @override
  void registerSerializer(Type type, Ser ser) {
    _xtypeResolver.registerSerializer(type, ser);
  }

  @override
  @inline
  Object? fromFury(Uint8List bytes, [ByteReader? reader]) {
    return _deserDirector.deser(bytes, _conf, _xtypeResolver, reader);
  }

  @override
  @inline
  Uint8List toFury(Object? obj,) {
    return _serDirector.ser(obj, _conf, _xtypeResolver);
  }

  @override
  @inline
  void toFuryWithWriter(Object? obj, ByteWriter writer) {
    _serDirector.serWithWriter(obj, _conf, _xtypeResolver, writer);
  }

  /*-----For test only------------------------------------------------*/
  StructHashPair getStructHashPair(Type type) {
    return _xtypeResolver.getHashPairForTest(type);
  }
}