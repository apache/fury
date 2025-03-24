import 'package:fury/src/codegen/exception/fury_codegen_exception.dart';

abstract class SingleFieldExcep extends FuryCodegenException{
  final String _libPath;
  final String _className;
  final String _fieldName;

  SingleFieldExcep(this._libPath, this._className, this._fieldName, [super.where]);

  @override
  void giveExceptionMessage(StringBuffer buf) {
    super.giveExceptionMessage(buf);
    buf.write('related class: ');
    buf.write(_libPath);
    buf.write('@');
    buf.write(_className);
    buf.write('\n');
    buf.write('invalidField: ');
    buf.write(_fieldName,);
    buf.write('\n');
  }
}