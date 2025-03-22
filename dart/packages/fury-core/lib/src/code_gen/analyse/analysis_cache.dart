import 'dart:collection';
import 'package:fury_core/src/code_gen/entity/fields_cache_unit.dart';
import 'package:fury_core/src/code_gen/meta/impl/type_immutable.dart';
import 'package:fury_core/src/code_gen/meta/lib_import_pack.dart';
import 'package:fury_core/src/dev_annotation/donot_modify.dart';

import '../entity/contructor_params.dart';

class AnalysisCache{
  static final Map<int, ConstructorParams> _clsIdToUnnamedCons = HashMap();
  static final Map<int, FieldsCacheUnit> _clsIdToFields = HashMap();

  // _typeToTypeSpecUnnullable 和_typeToTypeSpec1Unnullable 这里只是记录分析过的类型，但是nullable信息不在其中，
  // 所以即使相关Analyzer在这里的缓存中找到了，nullable信息还是得自己去获取，而不能依赖提取出的缓存
  // static final Map<int, TypeSpecGen> _typeToTypeSpecUnnullable = HashMap();
  // static final Map<int, TypeSpecGen1> _typeToTypeSpec1Unnullable = HashMap();

  // 每个文件有自己的imports, 这里以文件(lib)为单位
  static final Map<int, LibImportPack> _libToImport = HashMap();

  static final Map<int, TypeImmutable> _typeId2TypeImmutable = HashMap();


  // static final Map<int, FieldType1CacheUnit> _fieldId2Type1CacheUnit = HashMap();

  // static final Map<int, FuryMeta> _type2FuryMeta = HashMap();

  /*--------------------------------------------------------------------*/
  @doNotModifyReturn
  static ConstructorParams? getUnnamedCons(int id){
    return _clsIdToUnnamedCons[id];
  }
  static void putUnnamedCons(int id, ConstructorParams params){
    _clsIdToUnnamedCons[id] = params;
  }
  /*--------------------------------------------------------------------*/
  @doNotModifyReturn
  static FieldsCacheUnit? getFields(int id){
    return _clsIdToFields[id];
  }

  static void putFields(int id, FieldsCacheUnit unit){
    _clsIdToFields[id] = unit;
  }

  // static FieldType1CacheUnit? getFieldType1CacheUnit(int id){
  //   return _fieldId2Type1CacheUnit[id];
  // }
  //
  // static void putFieldType1CacheUnit(int id, FieldType1CacheUnit unit){
  //   _fieldId2Type1CacheUnit[id] = unit;
  // }

  /*-------------------type-------------------------------------------------*/
  // static TypeSpecGen? getTypeSpec(int id){
  //   return _typeToTypeSpecUnnullable[id];
  // }
  //
  // static void putTypeSpec(int id, TypeSpecGen typeSpec){
  //   // 这里仅仅索引没有genericArgs的TypeSpecGen，
  //   // 如果要支持genericArgs的TypeSpecGen， 则Map起来较为麻烦，还要考虑嵌套的泛型类型
  //   // 而这本身也进行了分析过程， 所以现在仅支持没有genericArgs的TypeSpecGen
  //   assert(typeSpec.genericsArgs.isEmpty);
  //   _typeToTypeSpecUnnullable[id] = typeSpec;
  // }
  //
  // static TypeSpecGen1? getTypeSpec1(int id){
  //   return _typeToTypeSpec1Unnullable[id];
  // }
  //
  // static void putTypeSpec1(int id, TypeSpecGen1 typeSpec){
  //   _typeToTypeSpec1Unnullable[id] = typeSpec;
  // }

  static TypeImmutable? getTypeImmutable(int id){
    return _typeId2TypeImmutable[id];
  }

  static void putTypeImmutable(int id, TypeImmutable typeImmutable){
    _typeId2TypeImmutable[id] = typeImmutable;
  }

  /*-----------------lib imports---------------------------------------------------*/
  @doNotModifyReturn
  static LibImportPack? getLibImport(int id){
    return _libToImport[id];
  }

  static void putLibImport(int id, LibImportPack libImport){
    _libToImport[id] = libImport;
  }
  /*------------------furyMeta--------------------------------------------------*/
  // static FuryMeta? getFuryMeta(int classId){
  //   return _type2FuryMeta[classId];
  // }
  // static void putFuryMeta(int classId, FuryMeta meta){
  //   _type2FuryMeta[classId] = meta;
  // }
}