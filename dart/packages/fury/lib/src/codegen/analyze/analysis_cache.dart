import 'dart:collection';
import 'package:fury/src/codegen/entity/contructor_params.dart';
import 'package:fury/src/codegen/entity/fields_cache_unit.dart';
import 'package:fury/src/codegen/meta/impl/type_immutable.dart';
import 'package:fury/src/codegen/meta/lib_import_pack.dart';

class AnalysisCache{
  static final Map<int, ConstructorParams> _clsIdToUnnamedCons = HashMap();
  static final Map<int, FieldsCacheUnit> _clsIdToFields = HashMap();

  // Each file has its own imports, treated here as a unit (lib) per file
  static final Map<int, LibImportPack> _libToImport = HashMap();

  static final Map<int, TypeImmutable> _typeId2TypeImmutable = HashMap();

  /*--------------------------------------------------------------------*/
  static ConstructorParams? getUnnamedCons(int id){
    return _clsIdToUnnamedCons[id];
  }
  // the modification of the return value is not allowed
  static void putUnnamedCons(int id, ConstructorParams params){
    _clsIdToUnnamedCons[id] = params;
  }
  /*--------------------------------------------------------------------*/
  // the modification of the return value is not allowed
  static FieldsCacheUnit? getFields(int id){
    return _clsIdToFields[id];
  }

  static void putFields(int id, FieldsCacheUnit unit){
    _clsIdToFields[id] = unit;
  }

  static TypeImmutable? getTypeImmutable(int id){
    return _typeId2TypeImmutable[id];
  }

  static void putTypeImmutable(int id, TypeImmutable typeImmutable){
    _typeId2TypeImmutable[id] = typeImmutable;
  }

  /*-----------------lib imports---------------------------------------------------*/
  // the modification of the return value is not allowed
  static LibImportPack? getLibImport(int id){
    return _libToImport[id];
  }

  static void putLibImport(int id, LibImportPack libImport){
    _libToImport[id] = libImport;
  }
}