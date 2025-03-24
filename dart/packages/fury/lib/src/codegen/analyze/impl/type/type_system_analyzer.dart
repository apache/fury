import 'package:analyzer/dart/element/element.dart';
import 'package:analyzer/dart/element/type.dart';
import 'package:fury/src/codegen/analyze/analysis_type_identifier.dart';
import 'package:fury/src/codegen/analyze/analyzer.dart';
import 'package:fury/src/codegen/analyze/annotation/location_level_ensure.dart';
import 'package:fury/src/codegen/analyze/entity/analysis_res_wrap.dart';
import 'package:fury/src/codegen/analyze/entity/obj_type_res.dart';
import 'package:fury/src/codegen/const/location_level.dart';
import 'package:fury/src/codegen/entity/either.dart';
import 'package:fury/src/codegen/entity/location_mark.dart';
import 'package:fury/src/codegen/exception/unsupported_type_exception.dart';
import 'package:fury/src/const/dart_type.dart';

class TypeSystemAnalyzer{

  const TypeSystemAnalyzer();

  ObjTypeRes analyzeObjType(
    InterfaceElement element,
    @LocationEnsure(LocationLevel.fieldLevel)LocationMark locationMark,
  ){
    assert(locationMark.ensureFieldLevel);
    // 确认现在的ObjType
    Either<ObjTypeRes, DartTypeEnum> res = Analyzer.customTypeAnalyzer.analyzeType(element);
    if (res.isRight){
      throw UnsupportedTypeException(
        locationMark.libPath,
        locationMark.clsName,
        locationMark.fieldName!,
        res.right!.scheme,
        res.right!.path,
        res.right!.typeName,
      );
    }
    return res.left!;
  }

  TypeDecision decideInterfaceType(DartType inputType){
    InterfaceType? type;
    DartType? dartType;
    if (inputType is InterfaceType){
      type = inputType;
    } else if (inputType.element is TypeParameterElement){
      dartType = (inputType.element as TypeParameterElement).bound;
      if (dartType is InterfaceType){
        type = dartType;
      }else if(dartType == null){
        // do nothing
      }else{
        throw ArgumentError(
          'Field type is not InterfaceType or DynamicType: $inputType',
        );
      }
    }else if(inputType is DynamicType){
      type = null;
    }
    else{
      throw ArgumentError(
        'Field type is not InterfaceType or TypeParameterElement: $inputType',
      );
    }
    return (type == null) ? (type: AnalysisTypeIdentifier.objectType, forceNullable: true): (type: type, forceNullable: false);
  }
}