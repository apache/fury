import 'package:fury_core/src/code_gen/excep/field_excep.dart';
import 'package:fury_core/src/code_gen/excep/field_access/field_access_error_type.dart';

class FieldAccessException extends FieldExcep{
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