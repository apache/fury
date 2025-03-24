import 'package:fury/src/codegen/exception/fury_codegen_exception.dart';

class ClassLevelException extends FuryCodegenException {

  final String _libPath;
  final String _className;

  ClassLevelException(this._libPath, this._className, [super._where]);

  @override
  void giveExceptionMessage(StringBuffer buf) {
    super.giveExceptionMessage(buf);
    buf.write('related class: ');
    buf.write(_libPath);
    buf.write('@');
    buf.write(_className);
    buf.write('\n');
  }


  @override
  String toString() {
    final buf = StringBuffer();
    giveExceptionMessage(buf);
    return buf.toString();
  }
}