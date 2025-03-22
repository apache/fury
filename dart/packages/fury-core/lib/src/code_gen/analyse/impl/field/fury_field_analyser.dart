// 每个field都会有setter和getter, 这里不是指OOP概念中公开的getter和setter
// 私有字段及时没有getter和setter, 也会有私有的getter和setter，只是外部不可见
// 类似如果使用set关键字定义了setter,例如
// set afield(int f) => _f = f;
// 这里的afield会被分析成一个fieldElement, 他此时只有setter
// 如果同时声明了同名getter, 例如
// int get afield => _f;
// 那么这个fieldElement(afield)会有getter和setter
// tips: dart中不允许field和getter/setter同名

import 'package:analyzer/dart/element/element.dart';
import 'package:fury_core/src/code_gen/meta/impl/type_spec_gen.dart';

import '../../../../../fury_core.dart';
import '../../../const/location_level.dart';
import '../../../entity/either.dart';
import '../../../entity/location_mark.dart';
import '../../../excep/fury_gen_excep.dart';
import '../../../meta/impl/field_spec_immutable.dart';
import '../../../meta/public_accessor_field.dart';
import '../../analyzer.dart';
import '../../annotation/location_level_ensure.dart';
import '../../interface/field_analyser.dart';

class FuryFieldAnalyser implements FieldAnalyser{

  const FuryFieldAnalyser();

  @override
  Either<FieldSpecImmutable, PublicAccessorField>? analyse(
    FieldElement element,
    FieldOverrideChecker fieldOverrideChecker,
    @LocationEnsure(LocationLevel.clsLevel) LocationMark locationMark,
  ) {
    assert(locationMark.ensureClassLevel);
    assert(!element.isStatic);

    String fieldName = element.name;
    /*---------字段重写检查--------------------------------------------*/
    if (fieldOverrideChecker(fieldName)){
      throw FuryGenExcep.fieldOverriding(
        locationMark.libPath,
        locationMark.clsName,
        fieldName,
      );
    }
    /*---------处理合成字段--------------------------------------------*/
    if(element.isSynthetic){
      // 合成字段，在这里就是getter和setter合成
      if (element.isPublic){
        return Either.right(
          PublicAccessorField(
            fieldName,
            element.setter != null,
            element.getter != null
          ),
        );
      }
      // 合成字段是私有的
      return null;
    }
    /*---------位置记录--------------------------------------------*/
    locationMark = locationMark.copyWithFieldName(fieldName); // 注意在这里加入fieldName

    FuryKey key = Analyzer.keyAnnotationAnalyzer.analyze(
      element.metadata,
      locationMark,
    );

    // 注意，这一步把关不能比下面的操作晚，
    // 因为下面的操作是有检查的，如果这里不管是否完全的排除了的话
    // 如果实际上这里确实既不需要序列化也不需要反序列化，下面的检查还是会有，相当于检查这些不包含的字段
    if (!key.includeFromFury && !key.includeToFury){
      // 如果两个都为false, 直接返回null，此字段直接忽视
      return null;
    }

    TypeSpecGen fieldType = Analyzer.typeAnalyser.getTypeImmutableAndTag(
      Analyzer.typeSystemAnalyzer.decideInterfaceType(element.type),
      // key.serializeTo,
      // key.deserializeFrom,
      locationMark,
    );

    return Either.left(
      FieldSpecImmutable.publicOr(
        element.isPublic,
        name: fieldName,
        typeSpec: fieldType,
        className: locationMark.clsName,
        isFinal: element.isFinal,
        isLate: element.isLate,
        hasInitializer: element.hasInitializer,
        includeFromFury: key.includeFromFury,
        includeToFury: key.includeToFury,
      ),
    );
  }
}