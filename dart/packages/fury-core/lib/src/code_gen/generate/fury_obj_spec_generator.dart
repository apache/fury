import 'package:analyzer/dart/element/element.dart';
import 'package:build/build.dart';
import 'package:fury_core/src/annotation/fury_class.dart';
import 'package:fury_core/src/annotation/fury_enum.dart';
import 'package:fury_core/src/code_gen/analyse/analysis_type_identifier.dart';
import 'package:fury_core/src/code_gen/analyse/analyzer.dart';
import 'package:fury_core/src/code_gen/excep/fury_gen_excep.dart';
import 'package:fury_core/src/code_gen/meta/gen_export.dart';
import 'package:source_gen/source_gen.dart';

import '../../annotation/fury_obj.dart';

class FuryObjSpecGenerator extends GeneratorForAnnotation<FuryObj> {

  static int? furyEnumId;
  static int? furyClassId;

  @override
  Future<String> generateForAnnotatedElement(
      Element element,
      ConstantReader annotation,
      BuildStep buildStep) async {

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
        throw FuryGenExcep.falseAnnotationTarget(
          FuryEnum.name,
          element.displayName,
          FuryEnum.targets,
        );
      }
      spec = Analyzer.enumAnalyzer.analyze(element);
    }else {
      if (element is! ClassElement) {
        throw FuryGenExcep.falseAnnotationTarget(
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