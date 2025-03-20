// other fields sorting method.
// List<MapEntry<FieldSpecGen, String>> it = fields.map(
//   (f) => MapEntry(f, NamingStyleUtil.lowerCamelToLowerUnderscore(f.pureName))
// ).toList(growable: false)
//   ..sort((a, b) => a.value.compareTo(b.value));
// fields = it.map((e) => e.key).toList(growable: false);

import 'dart:collection';

import 'package:fury_core/src/code_gen/analyse/analyzer.dart';
import 'package:fury_core/src/code_gen/analyse/annotation/location_level_ensure.dart';
import 'package:fury_core/src/code_gen/const/location_level.dart';
import 'package:fury_core/src/code_gen/entity/location_mark.dart';
import 'package:fury_core/src/code_gen/excep/field_access/field_access_error_type.dart';
import 'package:fury_core/src/code_gen/meta/impl/fields_spec_gen.dart';
import 'package:fury_core/src/dev_annotation/maybe_modified.dart';

import '../../../entity/contructor_params.dart';
import '../../../excep/fury_gen_excep.dart';
import '../../../meta/impl/field_spec_immutable.dart';

class AccessInfoAnalyzer {

  const AccessInfoAnalyzer();

  // 前提是fields已经排序过了
  FieldsSpecGen  _analyzeConsAndFields(
    ConstructorParams consParams,
    List<FieldSpecImmutable> fields,
    bool fieldsSorted,
    @LocationEnsure(LocationLevel.clsLevel) LocationMark locationMark,
  ){
    assert(locationMark.ensureClassLevel);
    assert(fieldsSorted);

    Map<String, int> fieldNameToIndex = HashMap();
    for (int i = 0; i < fields.length; ++i){
      fieldNameToIndex[fields[i].name] = i;
    }
    List<bool> setThroughConsFlags = List.filled(fields.length, false);
    int? index;
    for (var consParam in consParams.iterator){
      index = fieldNameToIndex[consParam.name];
      assert(index != null, "Field ${consParam.name} not found in fields"); // 根据前面对于Constructor的限制，这里不不可能找不到index
      if (!(fields[index!].includeFromFury)){
        // 说明这个字段不在fields中或者不需要被分析
        // 但是如果consParam是optional, it's ok
        if (consParam.optional) {
          consParam.setNotInclude();
          continue;
        }
        // 否则就报错
        throw FuryGenExcep.fieldAccessError(
          clsLibPath: locationMark.libPath,
          clsName: locationMark.clsName,
          fieldName: [fields[index].name],
          accessType: FieldAccessErrorType.notIncludedButConsDemand,
        );
      }
      consParam.setFieldIndex(index);  // // 可以发现, 每个consParam都对应一个field， 也全都会被设置index
      setThroughConsFlags[index] = true;
    }

    return FieldsSpecGen(fields, fieldsSorted, setThroughConsFlags);
  }


  FieldsSpecGen checkAndSetTheAccessInfo(
    @mayBeModified ConstructorParams? consParams,
    List<FieldSpecImmutable> fieldImmutables,
    bool fieldsSorted,
    @LocationEnsure(LocationLevel.clsLevel) LocationMark locationMark,
  ){
    assert(locationMark.ensureClassLevel);
    if (consParams == null){
      // 说明没有constructor
      return FieldsSpecGen(fieldImmutables, fieldsSorted, List.filled(fieldImmutables.length, false));
    }
    /*-----------sort field--------------------------------------------------------------*/
    if (!fieldsSorted){
      // 说明没有排序
      Analyzer.fieldsSorter.sortFieldsByName(fieldImmutables);
      fieldsSorted = true;
    }
    /*------------set consParams----------------------------------------------------------*/
    FieldsSpecGen fieldsSpecGen =  _analyzeConsAndFields(consParams, fieldImmutables, fieldsSorted, locationMark);
    /*---------------检查是否有includeFromFury 且没有赋值方法的字段-----------------------------*/
    List<String> noWayFields = [];
    var immutables = fieldsSpecGen.fields;
    var flags = fieldsSpecGen.setThroughConsFlags;

    for (int i =0;i<immutables.length;++i){
      if (immutables[i].includeFromFury && !(immutables[i].canSet) && !flags[i]){
        noWayFields.add(immutables[i].name);
      }
    }
    if (noWayFields.isNotEmpty){
      // 说明有字段没有赋值方法
      throw FuryGenExcep.fieldAccessError(
        clsLibPath: locationMark.libPath,
        clsName: locationMark.clsName,
        fieldName: noWayFields,
        accessType: FieldAccessErrorType.noWayToAssign,
      );
    }
    /*---------------检查是否有includeToFury 且没有get途径的字段-----------------------------*/
    noWayFields.clear();
    for (int i =0;i<immutables.length;++i){
      if (immutables[i].includeToFury && !(immutables[i].canGet)){
        noWayFields.add(immutables[i].name);
      }
    }
    if (noWayFields.isNotEmpty){
      // 说明有字段没有赋值方法
      throw FuryGenExcep.fieldAccessError(
        clsLibPath: locationMark.libPath,
        clsName: locationMark.clsName,
        fieldName: noWayFields,
        accessType: FieldAccessErrorType.noWayToGet,
      );
    }
    return fieldsSpecGen;
  }
}