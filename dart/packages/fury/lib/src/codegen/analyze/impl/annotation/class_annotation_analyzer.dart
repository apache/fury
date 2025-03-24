import 'package:analyzer/dart/constant/value.dart';
import 'package:analyzer/dart/element/element.dart';
import 'package:fury/src/annotation/fury_class.dart';
import 'package:fury/src/codegen/analyze/analysis_type_identifier.dart';
import 'package:fury/src/codegen/analyze/annotation/location_level_ensure.dart';
import 'package:fury/src/codegen/const/location_level.dart';
import 'package:fury/src/codegen/entity/location_mark.dart';
import 'package:fury/src/codegen/exception/annotation_exception.dart';

class ClassAnnotationAnalyzer {

  const ClassAnnotationAnalyzer();

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
          throw DuplicatedAnnotationException(FuryClass.name, locationMark.clsName, locationMark.libPath);
        }
        getFuryClass = true;
      }
    }
    assert (getFuryClass); // There must be a FuryMeta annotation, otherwise this class would not be analyzed
    bool promiseAcyclic = anno.getField("promiseAcyclic")!.toBoolValue()!;
    return FuryClass(
      promiseAcyclic: promiseAcyclic,
    );
  }

  // This method does not check the validity of the annotation, nor does it perform caching operations
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
