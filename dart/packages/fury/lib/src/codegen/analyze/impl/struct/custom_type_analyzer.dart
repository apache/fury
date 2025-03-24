import 'package:analyzer/dart/element/element.dart';
import 'package:fury/src/codegen/analyze/analyzer.dart';
import 'package:fury/src/codegen/analyze/entity/obj_type_res.dart';
import 'package:fury/src/codegen/entity/either.dart';
import 'package:fury/src/codegen/exception/annotation_exception.dart';
import 'package:fury/src/const/dart_type.dart';

class CustomTypeAnalyzer {

  const CustomTypeAnalyzer();
  /*
   Here is an explanation
   Why, when this class is a custom type,
   it simply retrieves without checking validity or caching the analysis results for possible future operations.
   First, if this class has annotations, it will naturally trigger a dedicated analysis. If there is an error in the writing, it will naturally report it, so why not let it handle it there.
   Additionally, for now, annotations are simple and it seems possible to analyze everything at once, but in the future, as fields may gradually increase, handling it here would be overstepping, so it is not cached now.
   */
  // Here, the left of either is the result found without prohibition, and the right is the prohibited type.
  Either<ObjTypeRes, DartTypeEnum> analyzeType(InterfaceElement element){
    String name = element.name;
    Uri libLoc = element.library.source.uri;
    String scheme = libLoc.scheme;
    String path = libLoc.path;

    DartTypeEnum? dartTypeEnum = DartTypeEnum.find(name, scheme, path);

    if (dartTypeEnum != null) {
      if (dartTypeEnum.objType == null) {
        return Either.right(dartTypeEnum);
      }
      return Either.left(
        ObjTypeRes(dartTypeEnum.objType!, dartTypeEnum.certainForSer)
      );
    }
    // Not found in built-in types, considered as a custom type
    if (element is EnumElement){
      if (!Analyzer.enumAnnotationAnalyzer.hasFuryEnumAnnotation(element.metadata)){
        throw CodegenUnregisteredTypeException(path, name, 'FuryEnum');
      }
      return Either.left(ObjTypeRes.namedEnum);
    }
    assert(element is ClassElement);
    if (Analyzer.classAnnotationAnalyzer.hasFuryClassAnnotation(element.metadata)){
      return Either.left(ObjTypeRes.namedStruct);
    }
    return Either.left(ObjTypeRes.unknownStruct);
  }

}
