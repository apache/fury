import 'package:fury_core/src/code_gen/excep/constraint_violation/constructor_param_excep.dart';
import 'package:fury_core/src/code_gen/excep/annotation/unsupported_annotation_target_excep.dart';
import 'package:fury_core/src/code_gen/excep/field_access/field_access_excep.dart';
import 'package:fury_core/src/code_gen/excep/unsupported_type_excep.dart';
import 'package:meta/meta_meta.dart';

import '../../excep/fury_exception.dart';
import 'annotation/class_tag_invalid_excep.dart';
import 'annotation/duplicated_annotation_excep.dart';
import 'constraint_violation/field_overriding_excep.dart';
import 'field_access/field_access_error_type.dart';

abstract class FuryGenExcep extends FuryException {
  final String? _where;
  FuryGenExcep([this._where]);

  /// will generate warning and error location
  @override
  void giveExcepMsg(StringBuffer buf) {
    buf.write(
'''[FURY]: Analysis error detected!
You need to make sure your codes don't contain any grammar error itself.
And review the error messages below, correct the issues, and then REGENERATE the code.
''');
    if (_where != null && _where.isNotEmpty) {
      buf.write('where: ');
      buf.write(_where);
      buf.write('\n');
    }
  }

  static FuryException falseAnnotationTarget(String annotation, String theTarget, List<TargetKind> supported, [String? where]) {
    return UnsupportedAnnotationTargetExcep(annotation, theTarget, supported, where);
  }

  static FuryException invalidClassTag(List<String>? classesWithEmptyTag,  List<String>? classesWithTooLongTag, Map<String, List<String>>? repeatedTags, [String? where]) {
    return ClassTagInvalidExcep(classesWithEmptyTag, classesWithTooLongTag, repeatedTags, where);
  }

  static FuryException duplicatedAnnotation(String annotation, String displayName, [String? where]) {
    return DuplicatedAnnotationExcep(annotation, displayName, where);
  }

  // static FuryGenExcep noUnamedConstructor(String libPath, String className, [String? where]) {
  //   return ConstructorExcep(libPath, className, CodeRules.mustHaveUnnamed, where);
  // }

  // //TODO: Not sure 要不要加这个限制
  // static FuryGenExcep unsupportedConstructorRedirect(String libPath, String className, [String? where]) {
  //   return ClassLevelExcep(libPath, className, 'constructor redirect is not supported', where);
  // }

  static FuryGenExcep constructorParamInformal(String libPath, String className, List<String> invalidParams, [String? where]) {
    return ConstructorParamInformal(libPath, className, invalidParams, where);
  }

  /*-----------Unsupported Type----------------------------------------------------------------------------------------------------*/
  static FuryGenExcep unsupportedType({
    required String clsLibPath,
    required String clsName,
    required String fieldName,
    required String typeScheme,
    required String typePath,
    required String typeName,
  }) => UnsupportedTypeException(
      clsLibPath,
      clsName,
      fieldName,
      typeScheme,
      typePath,
      typeName,
    );

  /*----------field access error---------------------------------------------------------------------------------------------------*/

  static FuryGenExcep fieldAccessError({
    required String clsLibPath,
    required String clsName,
    required List<String> fieldName,
    required FieldAccessErrorType accessType,
  }) => FieldAccessException(
      clsLibPath,
      clsName,
      fieldName,
      accessType,
    );
  /*----------field access error---------------------------------------------------------------------------------------------------*/
  // static FuryGenExcep incompatibleTypeMapping(
  //   String libPath,
  //   String className,
  //   String fieldName,
  //   bool toOrFrom,
  //   FuryType specifiedType,
  //   List<DartTypeEnum> possibleDartTypes,
  //   [String? where,]) => IncompatibleTypeMappingException(
  //     libPath,
  //     className,
  //     fieldName,
  //     toOrFrom,
  //     specifiedType,
  //     possibleDartTypes,
  //     where,
  //   );
  /*----------field access error---------------------------------------------------------------------------------------------------*/
  static FuryGenExcep fieldOverriding(
    String libPath,
    String className,
    String fieldName,
    [String? where,]) => FieldOverridingException(
      libPath,
      className,
      [fieldName],
      where,
    );

  /*----------conflict annotation---------------------------------------------------------------------------------------------------*/
  // static FuryGenExcep conflictAnnotation(
  //   String targetOne,
  //   String conflictOne,
  //   [String? where,]) => ConflictAnnotationException(
  //     targetOne,
  //     conflictOne,
  //     where,
  //   );
  /*----------conflict annotation---------------------------------------------------------------------------------------------------*/
}
