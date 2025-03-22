import 'package:analyzer/dart/constant/value.dart';
import 'package:analyzer/dart/element/element.dart';
import 'package:fury_core/src/annotation/fury_class.dart';
import 'package:fury_core/src/code_gen/analyse/analysis_type_identifier.dart';
import 'package:fury_core/src/code_gen/entity/location_mark.dart';

import '../../../../annotation/fury_enum.dart';
import '../../../const/location_level.dart';
import '../../../excep/fury_gen_excep.dart';
import '../../annotation/location_level_ensure.dart';

class FuryEnumAnnotationAnalyzer {

  const FuryEnumAnnotationAnalyzer();

  FuryEnum analyze(
    List<ElementAnnotation> metadata,
    @LocationEnsure(LocationLevel.clsLevel)LocationMark locationMark,
  ){
    assert(locationMark.ensureClassLevel);
    late DartObject anno;
    late ClassElement annoClsElement;
    bool getFuryEnum = false;
    for (ElementAnnotation annoElement in metadata){
      anno = annoElement.computeConstantValue()!;
      annoClsElement = anno.type!.element as ClassElement;
      if (AnalysisTypeIdentifier.isFuryEnum(annoClsElement)){
        if (getFuryEnum){
          throw FuryGenExcep.duplicatedAnnotation(FuryClass.name, locationMark.clsName, locationMark.libPath);
        }
        getFuryEnum = true;
      }
    }
    assert (getFuryEnum); // 肯定会有FuryMeta注解，要不然也不会分析这个类了
    // String? tag = anno.getField("tag")?.toStringValue();
    // tag ??= locationMark.clsName; // if tag is not set, use class name as tag
    // TODO: 这里还在修改中，没有字段，但是仍然保持代码结构
    return FuryEnum();
  }

  // 此方法不会进行注解的合法性检查，也不会进行缓存操作
  bool hasFuryEnumAnnotation(List<ElementAnnotation> metadata) {
    DartObject? anno;
    for (ElementAnnotation annoElement in metadata){
      anno = annoElement.computeConstantValue()!;
      ClassElement annoClsElement = anno.type!.element as ClassElement;
      if (AnalysisTypeIdentifier.isFuryEnum(annoClsElement)){
        return true;
      }
    }
    return false;
  }
}