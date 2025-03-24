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