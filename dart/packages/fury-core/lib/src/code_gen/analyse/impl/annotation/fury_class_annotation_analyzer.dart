import 'package:analyzer/dart/constant/value.dart';
import 'package:analyzer/dart/element/element.dart';
import 'package:fury_core/src/annotation/fury_class.dart';
import 'package:fury_core/src/code_gen/analyse/analysis_type_identifier.dart';
import 'package:fury_core/src/code_gen/entity/location_mark.dart';

import '../../../../../fury_core.dart';
import '../../../const/location_level.dart';
import '../../../excep/fury_gen_excep.dart';
import '../../annotation/location_level_ensure.dart';

class FuryClassAnnotationAnalyzer {

  const FuryClassAnnotationAnalyzer();

  FuryClass analyze(
    List<ElementAnnotation> metadata,
    int classId,
    @LocationEnsure(LocationLevel.clsLevel)LocationMark locationMark,
  ){
    assert(locationMark.ensureClassLevel);
    late DartObject anno;
    late ClassElement annoClsElement;
    bool getFuryClass = false;
    for (ElementAnnotation annoElement in metadata){
      anno = annoElement.computeConstantValue()!;
      annoClsElement = anno.type!.element as ClassElement;
      if (AnalysisTypeIdentifier.isFuryClass(annoClsElement)){
        if (getFuryClass){
          throw FuryGenExcep.duplicatedAnnotation(FuryClass.name, locationMark.clsName, locationMark.libPath);
        }
        getFuryClass = true;
      }
    }
    assert (getFuryClass); // 肯定会有FuryMeta注解，要不然也不会分析这个类了
    bool promiseAcyclic = anno.getField("promiseAcyclic")!.toBoolValue()!;
    return FuryClass(
      promiseAcyclic: promiseAcyclic,
    );
  }

  // 此方法不会进行注解的合法性检查，也不会进行缓存操作
  bool hasFuryClassAnnotation(List<ElementAnnotation> metadata) {
    DartObject? anno;
    for (ElementAnnotation annoElement in metadata){
      anno = annoElement.computeConstantValue()!;
      ClassElement annoClsElement = anno.type!.element as ClassElement;
      if (AnalysisTypeIdentifier.isFuryClass(annoClsElement)){
        return true;
      }
    }
    return false;
  }
}