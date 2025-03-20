import 'package:fury_core/src/code_gen/excep/constraint_violation/fury_constraint_violation.dart';
import 'package:fury_core/src/code_gen/rules/code_rules.dart';

class CircularIncapableRisk extends FuryConstraintViolation {
  final String libPath;
  final String className;

  CircularIncapableRisk(this.libPath, this.className,)
      : super(CodeRules.circularReferenceIncapableRisk,);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('related class: ');
    buf.write(libPath);
    buf.write('@');
    buf.write(className);
    buf.write('\n');
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}