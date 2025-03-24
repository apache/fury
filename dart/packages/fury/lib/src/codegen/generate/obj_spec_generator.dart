import 'package:analyzer/dart/element/element.dart';
import 'package:build/build.dart';
import 'package:source_gen/source_gen.dart';
import 'package:fury/src/annotation/fury_object.dart';
import 'package:fury/src/codegen/analyze/analysis_type_identifier.dart';
import 'package:fury/src/codegen/analyze/analyzer.dart';
import 'package:fury/src/codegen/exception/annotation_exception.dart';
import 'package:fury/src/codegen/meta/gen_export.dart';
import 'package:fury/src/annotation/fury_enum.dart';
import 'package:fury/src/annotation/fury_class.dart';

class FuryObjSpecGenerator extends GeneratorForAnnotation<FuryObject> {

  static int? furyEnumId;
  static int? furyClassId;

  @override
  Future<String> generateForAnnotatedElement(
    Element element,
    ConstantReader annotation,
    BuildStep buildStep
  ) async {
    Element anno = annotation.objectValue.type!.element!;
    late GenExport spec;
    late bool enumOrClass;
    if (furyEnumId != null){
      enumOrClass = anno.id == furyEnumId;
    }else{
      if (furyClassId != null) {
        enumOrClass = anno.id != furyClassId;
      }else{
        if (anno.name == 'FuryClass'){
          enumOrClass = false;
          furyClassId = anno.id;
          AnalysisTypeIdentifier.giveFuryClassId(anno.id);
        }else{
          enumOrClass = true;
          furyEnumId = anno.id;
          AnalysisTypeIdentifier.giveFuryEnumId(anno.id);
        }
      }
    }

    if (enumOrClass){
      if (element is! EnumElement) {
        throw InvalidAnnotationTargetException(
          FuryEnum.name,
          element.displayName,
          FuryEnum.targets,
        );
      }
      spec = Analyzer.enumAnalyzer.analyze(element);
    }else {
      if (element is! ClassElement) {
        throw InvalidAnnotationTargetException(
          FuryClass.name,
          element.displayName,
          FuryEnum.targets,
        );
      }
      spec = Analyzer.classAnalyzer.analyze(element);
    }

    StringBuffer buf = StringBuffer();
    spec.genCode(buf);
    return buf.toString();
  }
}