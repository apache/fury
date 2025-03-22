import 'package:fury_core/src/code_gen/excep/fury_gen_excep.dart';

abstract class SingleFieldExcep extends FuryGenExcep{
  final String _libPath;
  final String _className;
  final String _fieldName;

  SingleFieldExcep(this._libPath, this._className, this._fieldName, [super.where]);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
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