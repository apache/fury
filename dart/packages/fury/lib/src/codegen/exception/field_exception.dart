import 'package:fury/src/codegen/exception/constraint_violation_exception.dart';

abstract class FieldException extends FuryConstraintViolation {
  final String _libPath;
  final String _className;
  final List<String> _invalidFields;

  FieldException(this._libPath, this._className, this._invalidFields, super._constraint, [super.where]);

  @override
  void giveExceptionMessage(StringBuffer buf) {
    super.giveExceptionMessage(buf);
    buf.write('related class: ');
    buf.write(_libPath);
    buf.write('@');
    buf.write(_className);
    buf.write('\n');
    buf.write('invalidFields: ');
    buf.writeAll(_invalidFields, ', ');
    buf.write('\n');
  }

  @override
  String toString() {
    StringBuffer buf = StringBuffer();
    giveExceptionMessage(buf);
    return buf.toString();
  }
}

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