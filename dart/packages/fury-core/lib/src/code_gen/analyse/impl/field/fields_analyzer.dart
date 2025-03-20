import 'package:analyzer/dart/element/element.dart';
import 'package:fury_core/src/code_gen/analyse/annotation/location_level_ensure.dart';
import 'package:fury_core/src/code_gen/const/location_level.dart';
import 'package:fury_core/src/code_gen/entity/fields_cache_unit.dart';
import 'package:fury_core/src/code_gen/entity/location_mark.dart';
import 'package:fury_core/src/code_gen/meta/impl/field_spec_immutable.dart';

import '../../../meta/public_accessor_field.dart';
import '../../analysis_cache.dart';
import 'non_static_field_visitor.dart';

class FieldsAnalyzer{

  const FieldsAnalyzer();

  FieldsCacheUnit analyseFields(
    ClassElement element,
    @LocationEnsure(LocationLevel.clsLevel) LocationMark locationMark,
  ){
    assert(locationMark.ensureClassLevel);
    assert (element.supertype != null); // 至少也是Object
    FieldsCacheUnit res = _analyseFieldsInner(element, locationMark)!;
    return res;
  }

  /// 为了效率， 这里可以提供key(因为key可能外部已经建立过了，这里如果可以重用就重用)
  FieldsCacheUnit? _analyseFieldsInner(
    ClassElement element,
    @LocationEnsure(LocationLevel.clsLevel) LocationMark locationMark,
  ){
    assert (locationMark.ensureClassLevel);
    if (element.supertype == null) return null; // 继承链到了Object就不用分析了

    FieldsCacheUnit? cacheUnit = AnalysisCache.getFields(element.id);
    if (cacheUnit != null) {
      return cacheUnit;
    }

    FieldsCacheUnit? superCacheUnit = _analyseFieldsInner(element.supertype!.element as ClassElement, locationMark);

    List<FieldSpecImmutable>? superFields = superCacheUnit?.fieldImmutables;
    Set<String>? superParamNames = superCacheUnit?.fieldNames;

    // 开始分析现在类的fields
    NonStaticFieldVisitor visitor = NonStaticFieldVisitor(superParamNames, locationMark);
    element.visitChildren(visitor);
    // analyse setter and getter
    _analyzeFieldSetAndGet(visitor.fields, visitor.accessors);
    // put this and super together
    List<FieldSpecImmutable> fields = visitor.fields;
    Set<String> fieldNames = fields.map((e) => e.name).toSet();
    if (superCacheUnit != null){
      fields.addAll(superFields!);
      fieldNames.addAll(superParamNames!);
    }

    bool superAllFieldIndependent = (superCacheUnit == null) || (superCacheUnit.allFieldIndependent);
    late bool allFieldIndependent;
    if (!superAllFieldIndependent){
      allFieldIndependent = false;
    }else{
      allFieldIndependent = true;
      for (var field in fields){
        if (!field.typeSpec.independent){
          allFieldIndependent = false;
          break;
        }
      }
    }
    cacheUnit = FieldsCacheUnit(fields, allFieldIndependent, fieldNames);
    // cache
    AnalysisCache.putFields(element.id, cacheUnit);
    return cacheUnit;
  }


  // 此方法只是通过setter和getter,分析字段的刻读和可写性，但是例如字段finalAndHasInitializer, 这里的setter也改变不了它canSet=false的事实
  // 修改fields
  void _analyzeFieldSetAndGet(List<FieldSpecImmutable> fields, List<PublicAccessorField> accessors){
    // TODO： 这里采用了二分查找的方式来提高效率， 但是需要传入函数，并且排序本身也需要传入函数， 排序也要耗时，如果accessors不多， 可能反而效率不如顺序查找，这里先这样吧
    accessors.sort((a,b) => a.name.compareTo(b.name));
    for (var field in fields){
      if (field.isPublic) {
        assert(field.canGet);
        continue;
      }
      if (field.accessUnchangeable){
        continue;
      }
      final accessor = _searchAccessorByName(field.name.substring(1), accessors);
      if (accessor != null){
        field.notifyHasSetter(accessor.hasSetter);
        field.notifyHasGetter(accessor.hasGetter);
      }else{
        field.notifyHasGetter(false);
        field.notifyHasGetter(false);
      }
    }
  }

  PublicAccessorField? _searchAccessorByName(String name, List<PublicAccessorField> accessors){
    int low = 0;
    int high = accessors.length - 1;
    while (low <= high) {
      int mid = (low + high) >> 1;
      int cmp = accessors[mid].name.compareTo(name);
      if (cmp == 0) {
        return accessors[mid];
      } else if (cmp < 0) {
        low = mid + 1;
      } else {
        high = mid - 1;
      }
    }
    return null;
  }
}