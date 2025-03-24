import 'package:fury/src/codegen/exception/class_level_exception.dart';
import 'package:fury/src/codegen/exception/field_exception.dart';
import 'package:fury/src/codegen/exception/fury_codegen_exception.dart';
import 'package:fury/src/codegen/rules/code_rules.dart';

abstract class FuryConstraintViolation extends FuryCodegenException {
  final String _constraint;

  FuryConstraintViolation(this._constraint, [super._where]);

  @override
  void giveExceptionMessage(StringBuffer buf) {
    super.giveExceptionMessage(buf);
    buf.write('constraint: ');
    buf.write(_constraint);
    buf.write('\n');
  }
}

class CircularIncapableRisk extends FuryConstraintViolation {
  final String libPath;
  final String className;

  CircularIncapableRisk(this.libPath, this.className,)
      : super(CodeRules.circularReferenceIncapableRisk,);

  @override
  void giveExceptionMessage(StringBuffer buf) {
    super.giveExceptionMessage(buf);
    buf.write('related class: ');
    buf.write(libPath);
    buf.write('@');
    buf.write(className);
    buf.write('\n');
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExceptionMessage(buf);
    return buf.toString();
  }
}

class InformalConstructorParamException extends ClassLevelException {

  final List<String> _invalidParams;

  // There is no need to add the reason field, because the reason is actually just invalidParams
  InformalConstructorParamException(
      String libPath,
      String className,
      this._invalidParams,
      [String? where]): super(libPath, className, where);

  @override
  void giveExceptionMessage(StringBuffer buf) {
    super.giveExceptionMessage(buf);
    buf.write(CodeRules.consParamsOnlySupportThisAndSuper);
    buf.write('invalidParams: ');
    buf.writeAll(_invalidParams, ', ');
    buf.write('\n');
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExceptionMessage(buf);
    return buf.toString();
  }
}

class FieldOverridingException extends FieldException {
  FieldOverridingException(
      String libPath,
      String className,
      List<String> invalidFields,
      [String? where]
      ):
        super(libPath, className, invalidFields, CodeRules.unsupportFieldOverriding, where);
}

class NoUsableConstructorException extends FuryCodegenException {
  final String libPath;
  final String className;
  final reason;

  NoUsableConstructorException(this.libPath, this.className, this.reason)
      : super('$libPath@$className');
}

