import 'dart:collection';

import 'package:fury_core/src/code_gen/entity/struct_hash_pair.dart';
import 'package:fury_core/src/collection/key/long_long_key.dart';
import 'package:fury_core/src/const/obj_type.dart';
import 'package:fury_core/src/dev_annotation/optimize.dart';
import 'package:fury_core/src/excep/fury_exception.dart';
import 'package:fury_core/src/fury_context.dart';
import 'package:fury_core/src/memory/byte_reader.dart';
import 'package:fury_core/src/meta/class_info.dart';

import 'package:fury_core/src/meta/meta_string_byte.dart';
import 'package:fury_core/src/meta/spec_wraps/type_spec_wrap.dart';
import 'package:fury_core/src/resolver/dart_type_resolver.dart';
import 'package:fury_core/src/resolver/meta_str/meta_string_resolver.dart';
import 'package:fury_core/src/resolver/meta_str/tag_str_encode_resolver.dart';
import 'package:fury_core/src/resolver/struct_hash_resolver.dart';
import 'package:fury_core/src/ser/custom/class_ser.dart';
import 'package:fury_core/src/ser/custom/enum_ser.dart';
import 'package:fury_core/src/ser/ser.dart' show Ser;
import 'package:fury_core/src/util/string_util.dart';

import '../../excep/register/unregistered_type_excep.dart';
import '../../excep/unsupported_type_excep.dart';
import '../../memory/byte_writer.dart';
import '../../meta/specs/class_spec.dart';
import '../../meta/specs/custom_type_spec.dart';
import '../../ser_pack.dart';
import '../xtype_resolver.dart';

final class XtypeResolverImpl extends XtypeResolver {
  static const DartTypeResolver dartTypeResolver = DartTypeResolver.I;
  final FuryContext _ctx;
  final MetaStringResolver _msResolver;
  final TagStringEncodeResolver _tstrEncoder;
  final Map<LongLongKey, ClassInfo> _tagHash2Info;

  XtypeResolverImpl(
    super.conf,
  )
  : _tagHash2Info = HashMap<LongLongKey, ClassInfo>(),
    _msResolver = MetaStringResolver.newInst,
    _tstrEncoder = TagStringEncodeResolver.newInst,
    _ctx = FuryContext(conf)
  {
    _ctx.initForDefaultTypes();
  }

  @override
  void reg(CustomTypeSpec spec, [String? tag]) {
    if (tag == null){
      String typeName = spec.dartType.toString();
      _regWithNamespace(spec, typeName, typeName);
      return;
    }
    int idx = tag.lastIndexOf('.');
    if (idx == -1) {
      _regWithNamespace(spec, tag, tag);
    }else{
      String ns = tag.substring(0, idx);
      String tn = tag.substring(idx + 1);
      _regWithNamespace(spec, tag, tn, ns);
    }
  }

  @override
  void registerSerializer(Type type, Ser ser) {
    ClassInfo? clsInfo = _ctx.type2ClsInfo[type];
    if (clsInfo == null){
      throw UnregisteredTypeExcep(type);
    }
    clsInfo.ser = ser;
  }

  void _regWithNamespace(CustomTypeSpec spec, String tag, String tn, [String ns= '']) {
    assert(spec.objType == ObjType.NAMED_STRUCT || spec.objType == ObjType.NAMED_ENUM);
    MetaStringBytes tnMsb = _msResolver.getOrCreateMetaStringBytes(
      _tstrEncoder.encodeTn(tn),
    );
    MetaStringBytes nsMsb = _msResolver.getOrCreateMetaStringBytes(
      _tstrEncoder.encodeTn(ns),
    );
    ClassInfo classInfo = ClassInfo(
      spec.dartType,
      spec.objType,
      tag,
      tnMsb,
      nsMsb,
    );
    classInfo.ser = _getSerFor(spec);
    _ctx.reg(classInfo);
  }


  /// 这里产生的ClassSer, 对于其中的每一个TypeArg都不会分析其对应的ser
  /// 这有两方面的考虑，一是打算将具体的分析推迟到此Class的第一次解析，防止在一开始执行太多任务
  /// 二是，如果在这里将arg对应的Ser解析，在之后可能还会陆续注册很多Enum, 他们就无法在这里被识别，导致明明注册了却报没有注册的错误
  Ser _getSerFor(CustomTypeSpec spec) {
    if (spec.objType == ObjType.NAMED_ENUM){
      Ser ser = EnumSer.cache.getSerWithSpec(_ctx.conf, spec, spec.dartType);
      return ser;
    }
    // 说明是ClassSer
    return ClassSer.cache.getSerWithSpec(_ctx.conf, spec as ClassSpec, spec.dartType);
  }

  /// 此type一定是用户定义的class或enum
  @override
  @inline
  String getTagByCustomDartType(Type type) {
    String? tag = _ctx.type2ClsInfo[type]?.tag;
    if (tag == null){
      throw UnregisteredTypeExcep(type);
    }
    return tag;
  }

  @override
  void setSersForTypeWrap(List<TypeSpecWrap> typeWraps) {
    TypeSpecWrap wrap;
    for (int i = 0; i < typeWraps.length; ++i){
      wrap = typeWraps[i];
      if (wrap.certainForSer){
        wrap.ser = _ctx.type2ClsInfo[wrap.type]!.ser;
      }else if (wrap.objType == ObjType.LIST){
        wrap.ser = _ctx.abstractListSer;
      }else if (wrap.objType == ObjType.MAP) {
        wrap.ser = _ctx.abstractMapSer;
      }
      // 到此不设置ser, ser仍然是null
      setSersForTypeWrap(wrap.genericsArgs);
    }
  }

  @override
  ClassInfo readClassInfo(ByteReader br) {
    int xtypeId = br.readVarUint32Small14();
    ObjType xtype = ObjType.fromId(xtypeId)!;
    switch(xtype){
      case ObjType.NAMED_ENUM:
      case ObjType.NAMED_STRUCT:
      case ObjType.NAMED_COMPATIBLE_STRUCT:
      case ObjType.NAMED_EXT:
        MetaStringBytes pkgBytes = _msResolver.readMetaStringBytes(br);
        // assert(pkgBytes.length == 0); // fury dart不支持package
        MetaStringBytes simpleClassNameBytes = _msResolver.readMetaStringBytes(br);
        LongLongKey key = LongLongKey(pkgBytes.hashCode, simpleClassNameBytes.hashCode);
        ClassInfo? clsInfo = _tagHash2Info[key];
        if (clsInfo != null) {
          // 说明已经注册过了
          return clsInfo;
        }
        clsInfo = _getAndCacheSpecByBytes(key, pkgBytes, simpleClassNameBytes);
        // _tagHash2Info[key] = clsInfo;
        return clsInfo;
      default:
        // 说明是内置类型
        ClassInfo? clsInfo = _ctx.objTypeId2ClsInfo[xtypeId];
        if (clsInfo != null) {
          return clsInfo;
        } else {
          throw UnsupportedTypeExcep(xtype);
        }
    }
  }

  ClassInfo _getAndCacheSpecByBytes(
    LongLongKey key,
    MetaStringBytes packageBytes,
    MetaStringBytes simpleClassNameBytes,
  ) {
    String tn = _msResolver.decodeTypename(simpleClassNameBytes);
    String ns = _msResolver.decodeNamespace(packageBytes);
    String qualifiedName = StringUtil.addingTypeNameAndNs(ns, tn);
    ClassInfo? clsInfo = _ctx.tag2ClsInfo[qualifiedName];
    if (clsInfo == null) {
      // TODO: 不支持non-existent class, furyJava貌似有一定的支持
      throw FuryException.unregisteredExcep(qualifiedName);
    }
    _tagHash2Info[key] = clsInfo;
    return clsInfo;
  }

  @override
  ClassInfo writeGetClassInfo(ByteWriter bw, Object obj, SerPack pack){
    Type dartType = dartTypeResolver.getFuryType(obj);
    ClassInfo? clsInfo = _ctx.type2ClsInfo[dartType];
    if (clsInfo == null){
      throw UnregisteredTypeExcep(dartType);
    }
    bw.writeVarUint32Small7(clsInfo.objType.id);
    switch(clsInfo.objType){
      case ObjType.NAMED_ENUM:
      case ObjType.NAMED_STRUCT:
      case ObjType.NAMED_COMPATIBLE_STRUCT:
      case ObjType.NAMED_EXT:
        pack.msWritingResolver.writeMsb(bw, clsInfo.nsBytes!);
        pack.msWritingResolver.writeMsb(bw, clsInfo.typeNameBytes!);
        break;
      default:
        break;
    }
    return clsInfo;
  }


  /*-----For test only--------------------------------------------------------*/
  @override
  StructHashPair getHashPairForTest(Type type) {
    ClassInfo? clsInfo = _ctx.type2ClsInfo[type];
    if (clsInfo == null){
      throw UnregisteredTypeExcep(type);
    }
    ClassSer ser = clsInfo.ser as ClassSer;
    StructHashPair pair = ser.getHashPairForTest(
      StructHashResolver.inst,
      getTagByCustomDartType,
    );
    return pair;
  }
}