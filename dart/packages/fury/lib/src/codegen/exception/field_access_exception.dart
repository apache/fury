import 'package:fury/src/codegen/exception/field_exception.dart';

enum FieldAccessErrorType{
  noWayToAssign("This field needs to be assigned a value because it's includedFromFury, but it's not a constructor parameter and can't be assigned via a setter."),
  noWayToGet("This field needs to be read because it's includedFromFury, but it's not public and it can't be read via a getter."),
  notIncludedButConsDemand("This field is included in the constructor, but it's not includedFromFury. ");

  final String warning;

  const FieldAccessErrorType(this.warning);
}

class FieldAccessException extends FieldException {
  final FieldAccessErrorType errorType;

  FieldAccessException(
    String libPath,
    String clsName,
    List<String> fieldNames,
    this.errorType,
  ):super (
    libPath,
    clsName,
    fieldNames,
    errorType.warning,
  );
}