import 'package:fury_core/src/code_gen/excep/fury_gen_excep.dart';

abstract class FuryConstraintViolation extends FuryGenExcep {
  final String _constraint;

  FuryConstraintViolation(this._constraint, [super._where]);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('constraint: ');
    buf.write(_constraint);
    buf.write('\n');
  }
}