import 'package:analyzer/dart/constant/value.dart';
import 'package:analyzer/dart/element/element.dart';
import 'package:fury_core/src/code_gen/analyse/analysis_type_identifier.dart';
import 'package:fury_core/src/code_gen/entity/location_mark.dart';

import '../../../../../fury_core.dart';
import '../../../const/location_level.dart';
import '../../../excep/fury_gen_excep.dart';
import '../../annotation/location_level_ensure.dart';

class FuryKeyAnnotationAnalyzer {

  const FuryKeyAnnotationAnalyzer();

  // 目前FuryKey一次分析只会分析一次，不必使用缓存机制
  FuryKey analyze(
    List<ElementAnnotation> metadata,
    @LocationEnsure(LocationLevel.fieldLevel) LocationMark locationMark,
  ){
    assert(locationMark.ensureFieldLevel);
    DartObject? anno;
    late ClassElement annoClsElement;
    bool getMeta = false;
    for (ElementAnnotation annoElement in metadata){
      anno = annoElement.computeConstantValue()!;
      annoClsElement = anno.type!.element as ClassElement;
      if (AnalysisTypeIdentifier.isFuryKey(annoClsElement)){
        if (getMeta){
          throw FuryGenExcep.duplicatedAnnotation(
            FuryKey.name,
            locationMark.fieldName!,
            locationMark.clsLocation,
          );
        }
        getMeta = true;
        // 目前只处理FuryKey
      }
    }
    // metadata[0].element?.source?.contents;
    // get ignore info
    bool includeFromFury = true;
    bool includeToFury = true;
    // 如果没有注解，默认includeFromFury和includeToFury都为true
    if (anno != null){
      includeFromFury = anno.getField("includeFromFury")!.toBoolValue()!;
      includeToFury = anno.getField("includeToFury")!.toBoolValue()!;
      // serializeToVar = anno.getField("serializeTo")?.variable;
      // deserializeFromVar = anno.getField("deserializeFrom")?.variable;
      // if (serializeToVar != null){
      //   serializeTo = FuryType.fromString(serializeToVar.name)!;
      // }
      // if (deserializeFromVar != null){
      //   deserializeFrom = FuryType.fromString(deserializeFromVar.name)!;
      // }
      // targetType = (anno.getField("targetType")!.variable.enclosingElement3 as EnumElement).fields[1].;
    }
    FuryKey furyKey = FuryKey(
      // serializeTo: serializeTo,
      // deserializeFrom: deserializeFrom,
      includeFromFury: includeFromFury,
      includeToFury: includeToFury,
    );
    return furyKey;
  }
}