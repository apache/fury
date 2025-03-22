import 'package:fury_core/src/code_gen/excep/constraint_violation/fury_constraint_violation.dart';

abstract class FieldExcep extends FuryConstraintViolation {
  final String _libPath;
  final String _className;
  final List<String> _invalidFields;

  FieldExcep(this._libPath, this._className, this._invalidFields, super._constraint, [super.where]);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
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
    giveExcepMsg(buf);
    return buf.toString();
  }
}