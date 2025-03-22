// void checkTypeCompatible(
//   ObjType defaultType,
//   FuryType? serializeTo,
//   FuryType? deserializeFrom,
//   @LocationEnsure(LocationLevel.fieldLevel) LocationMark locationMark,
// ){
//   assert(locationMark.ensureFieldLevel);
//   // 这里就需要分析用户期望将defaultType视作userTargetType来序列化, 这里就要检查这种期望到底是否合法
//   if (serializeTo != null && !serializeTo.canBeTargetOf(defaultType)){
//     throw FuryGenExcep.incompatibleTypeMapping(
//       locationMark.libPath,
//       locationMark.clsName,
//       locationMark.fieldName!,
//       true,
//       serializeTo,
//       serializeTo.possibleTos,
//     );
//   }
//   if (deserializeFrom != null && !deserializeFrom.canBeSourceFor(defaultType)){
//     throw FuryGenExcep.incompatibleTypeMapping(
//       locationMark.libPath,
//       locationMark.clsName,
//       locationMark.fieldName!,
//       false,
//       deserializeFrom,
//       deserializeFrom.possibleFroms,
//     );
//   }
// }
import 'package:analyzer/dart/element/element.dart';
import 'package:analyzer/dart/element/type.dart';
import 'package:fury_core/src/code_gen/analyse/analysis_type_identifier.dart';
import 'package:fury_core/src/code_gen/analyse/entity/obj_type_res.dart' show ObjTypeRes;

import '../../../../const/dart_type.dart';
import '../../../const/location_level.dart';
import '../../../entity/either.dart';
import '../../../entity/location_mark.dart';
import '../../../excep/fury_gen_excep.dart';
import '../../analyzer.dart';
import '../../annotation/location_level_ensure.dart';
import '../../entity/analysis_res_wrap.dart';

class TypeSystemAnalyzer{

  const TypeSystemAnalyzer();

  ObjTypeRes analyzeObjType(
    InterfaceElement element,
    @LocationEnsure(LocationLevel.fieldLevel)LocationMark locationMark,
  ){
    assert(locationMark.ensureFieldLevel);
    // 确认现在的ObjType
    Either<ObjTypeRes, DartTypeEnum> res = Analyzer.customTypeAnalyzer.analyseType(element);
    if (res.isRight){
      throw FuryGenExcep.unsupportedType(
        clsLibPath: locationMark.libPath,
        clsName: locationMark.clsName,
        fieldName: locationMark.fieldName!,
        typeScheme: res.right!.scheme,
        typePath: res.right!.path,
        typeName: res.right!.typeName,
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