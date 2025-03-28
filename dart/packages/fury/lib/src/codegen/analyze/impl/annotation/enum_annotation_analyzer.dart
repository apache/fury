import 'package:analyzer/dart/constant/value.dart';
import 'package:analyzer/dart/element/element.dart';
import 'package:fury/src/annotation/fury_class.dart';
import 'package:fury/src/codegen/analyze/analysis_type_identifier.dart';
import 'package:fury/src/codegen/analyze/annotation/location_level_ensure.dart';
import 'package:fury/src/codegen/const/location_level.dart';
import 'package:fury/src/codegen/entity/location_mark.dart';
import 'package:fury/src/annotation/fury_enum.dart';
import 'package:fury/src/codegen/exception/annotation_exception.dart';

class EnumAnnotationAnalyzer {

  const EnumAnnotationAnalyzer();

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
          throw DuplicatedAnnotationException(FuryClass.name, locationMark.clsName, locationMark.libPath);
        }
        getFuryEnum = true;
      }
    }
    assert (getFuryEnum); // There will definitely be a FuryMeta annotation, otherwise this class wouldn't be analyzed.
    // TODO: This is still under modification, no fields yet, but maintaining the code structure.
    return FuryEnum();
  }

  // This method does not perform annotation validity checks, nor does it perform caching operations.
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