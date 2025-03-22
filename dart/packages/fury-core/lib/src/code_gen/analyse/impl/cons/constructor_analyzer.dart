// ConstructorParams analyse(
//   List<ConstructorElement> cons,
//   int classId,
//   @LocationEnsure(LocationLevel.clsLevel) LocationMark locationMark,
// ){
//   assert(locationMark.ensureClassLevel);
//   // 虽说通过ConstructorElement也可以找到对应的ClassId, 但是调用者直接提供吧
//   ConstructorElement? unnamedCons;
//   for (var consEle in cons){
//     if (consEle.name.isEmpty){
//       unnamedCons = consEle;
//       break;
//     }
//   }
//   if (unnamedCons == null){
//     throw FuryGenExcep.noUnamedConstructor(locationMark.libPath, locationMark.clsName);
//   }
//   assert(unnamedCons.superConstructor != null); // 现在分析的是一个类，不能是Object, Dart只有Object类没有super
//   final res = _analyseInner(unnamedCons, locationMark, classId, 0);
//   assert(res != null);
//   return res!;
// }
// 多个bool flag的解释
// isInitializingFormal: 是否是初始化参数,被this.修饰的参数(无论optional和named与否)
// isSuperFormal: 是否是super参数, 被super.修饰的参数(无论optional和named与否)
// isOptional: 是否是可选参数, optional的参数
// isNamed: 是否是命名参数, named的参数

// TODO：现在的分析函数是既不支持继承链中field override(dart官方也不推荐)
// TODO: 并且只关注UnnamedConstructor
// TODO: 并且UnnamedConstructor的参数只关心super和this修饰的参数
// TODO: 目前的限制就是以上，之后可以考虑支持某些
import 'package:analyzer/dart/constant/value.dart';
import 'package:analyzer/dart/element/element.dart';
import 'package:fury_core/src/code_gen/analyse/analysis_cache.dart';
import 'package:fury_core/src/code_gen/entity/constructor_param.dart';
import 'package:fury_core/src/code_gen/entity/contructor_params.dart';
import 'package:fury_core/src/code_gen/entity/location_mark.dart';
import 'package:fury_core/src/code_gen/excep/constraint_violation/circular_incapable_risk.dart';
import 'package:fury_core/src/code_gen/excep/constraint_violation/no_usable_cons_excep.dart';
import 'package:fury_core/src/code_gen/meta/impl/cons_info.dart';

import '../../../const/location_level.dart';
import '../../../excep/fury_gen_excep.dart';
import '../../analysis_type_identifier.dart';
import '../../annotation/location_level_ensure.dart';

class ConstrcutorAnalyzer {

  const ConstrcutorAnalyzer();

  ConstructorParams? _analyseInner(
    ConstructorElement element,
    @LocationEnsure(LocationLevel.clsLevel) LocationMark locationMark,
    [int? classId,
      int depth = 0]
  ){
    classId ??= element.enclosingElement3.id;
    // 每个被注解的类只会分析一次，一个类只有一个UnnamedConstructor， 所以对于depth = 0， 一定是第一次分析的类， 缓存中一定没有，也不必查了
    if (depth != 0){
      assert (depth > 0);
      if (element.superConstructor == null) return null; // 继承链到了Object就没什么好分析的了
      /*------------设置最终返回对象-------------------------*/
      // now got key. check cache
      ConstructorParams? cParams = AnalysisCache.getUnnamedCons(classId);
      if (cParams != null) return cParams;
    }

    ConstructorParams? superConsParams = _analyseInner(element.superConstructor!, locationMark, null, depth+1);
    final List<ConstructorParam>? superPositional = superConsParams?.positional;

    final params = element.parameters;
    final List<ConstructorParam> positional = [];
    final List<ConstructorParam> named = [];

    int superPositionalCount = 0;
    for (int i = 0; i < params.length; ++i){
      final param = params[i];
      if (param.isInitializingFormal){
        if (param.isPositional){
          positional.add(ConstructorParam.withName(param.name, param.isOptional));
        }else{
          // 说明是named
          assert(param.isNamed);
          named.add(ConstructorParam.withName(param.name, param.isOptional));
        }
      }else if (param.isSuperFormal){
        // 说明是super. 那么可以肯定superConsParams 不为null
        if (param.isPositional){
          positional.add(
              superPositional![superPositionalCount++].copyWithOptional(param.isOptional)
          ); // 一定要copy， 否则会导致不一致性
        }else{
          // 说明是named
          assert(param.isNamed);
          named.add(ConstructorParam.withName(param.name, param.isOptional));
        }
      }else {
        // 说明是普通参数
        if (param.isOptional){
          // TODO: 这里也许可以加强限制
          // 这里说明既不是初始化参数，也不是super参数，说明是普通参数，
          // 但是Optional的参数, 这里先不回抛出异常，但是不意味着检查过关，
          // 因为之后还要和fields一起在分析，是否所有需要的field都有机会赋值
          // TODO: 这里对于非异常情况(例如WARNING)的处理较简陋，之后可以尝试建立专门的log部件
          print("[WARNING] constructor param ${param.name} isn't initializing formal or super formal, but optional, please check");
        }else {
          throw FuryGenExcep.constructorParamInformal(
              locationMark.libPath,
              locationMark.clsName,
              [param.name,]
          );
        }
      }
    }
    // now got all params
    ConstructorParams cParams = ConstructorParams(
      positional,
      named,
    );
    AnalysisCache.putUnnamedCons(classId, cParams);
    return cParams;
  }


  ConstructorElement? _findUnnamedCons(
    List<ConstructorElement> cons,
  ){
    for (var consEle in cons){
      if (consEle.name.isEmpty){
        return consEle;
      }
    }
    return null;
  }

  ConstructorElement? _findSpecifiedCons(
    List<ConstructorElement> cons,
  ){
    late DartObject anno;
    late ClassElement annoClsElement;
    for (var consEle in cons){
      for (var annoEle in consEle.metadata){
        anno = annoEle.computeConstantValue()!;
        annoClsElement = anno.type!.element as ClassElement;
        if (AnalysisTypeIdentifier.isFuryCons(annoClsElement)){
          return consEle;
        }
      }
    }
    return null;
  }

  /// 寻找flexible constructor
  ConstructorElement? _findFlexibleCons(
    List<ConstructorElement> cons,
  ){
    for (var consEle in cons){
      if (_isFlexible(consEle)){
        return consEle;
      }
    }
    return null;
  }


  bool _isFlexible(
    ConstructorElement cons,
  ){
    for (final param in cons.parameters){
      if (param.isOptional) continue;
      return false;
    }
    return true;
  }


  void _checkCircularRisk(
    bool isFlex,
    bool allFieldsPrimitive,
    bool promiseAcyclic,
    @LocationEnsure(LocationLevel.clsLevel) LocationMark locationMark,
  ){
    if (isFlex || allFieldsPrimitive || promiseAcyclic) return;
    throw CircularIncapableRisk(
        locationMark.libPath,
        locationMark.clsName,
    );
  }



  ConsInfo analyse(
    List<ConstructorElement> cons,
    int classId,
    bool promiseAcyclic,
    bool allFieldsPrimitive,
    @LocationEnsure(LocationLevel.clsLevel) LocationMark locationMark,
  ){
    assert(locationMark.ensureClassLevel);
    // 寻找用户是否指定了constructor
    late bool isFlexible;
    ConstructorElement? consEle = _findSpecifiedCons(cons);
    if (consEle != null){
      isFlexible = _isFlexible(consEle);
      _checkCircularRisk(
        isFlexible,
        allFieldsPrimitive,
        promiseAcyclic,
        locationMark,
      );
    }else {
      consEle = _findFlexibleCons(cons);
      isFlexible = consEle != null;
      if (!isFlexible) {
        // 说明没有指定的constructor, 也没有flexible constructor
        // 那么就使用UnnamedConstructor
        consEle = _findUnnamedCons(cons);
        if (consEle == null) {
          throw NoUsableConsExcep(
            locationMark.libPath,
            locationMark.clsName,
            "You didn't specify a constructor using the @FuryCons annotation, "
            "but this class itself also doesn't have an Unnamed constructor "
            "or a constructor that takes no parameters, "
            "to the point that it can't continue analyzing it",
          );
        }else{
          // 找到了UnnamedConstructor，也只能使用这个了
          _checkCircularRisk(
            false,
            allFieldsPrimitive,
            promiseAcyclic,
            locationMark,
          );
        }
        isFlexible = false;
      }
    }
    if (isFlexible){
      return ConsInfo.useFlexibleCons(consEle.name);
    }
    // 说明是指定的constructor
    assert(consEle.superConstructor != null); // 现在分析的是一个类，不能是Object, Dart只有Object类没有super
    return ConsInfo.useUnnamedCons(
      _analyseInner(consEle, locationMark, classId, 0),
    );
  }

}