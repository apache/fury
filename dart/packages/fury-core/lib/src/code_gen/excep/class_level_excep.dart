import 'package:fury_core/src/code_gen/excep/fury_gen_excep.dart';

class ClassLevelExcep extends FuryGenExcep{

  final String _libPath;
  final String _className;

  ClassLevelExcep(this._libPath, this._className, [super._where]);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('related class: ');
    buf.write(_libPath);
    buf.write('@');
    buf.write(_className);
    buf.write('\n');
  }


  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}