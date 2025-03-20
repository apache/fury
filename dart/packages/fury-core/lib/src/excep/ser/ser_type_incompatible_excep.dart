import 'package:fury_core/src/const/obj_type.dart';
import 'package:fury_core/src/excep/ser/serialization_excep.dart';

class SerTypeIncompatibleExcep extends SerializationException {
  final ObjType _specified;
  final String _reason;

  SerTypeIncompatibleExcep(this._specified, this._reason, [super._where]);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('the specified type: ');
    buf.writeln(_specified);
    buf.write('while the reason: ');
    buf.writeln(_reason);
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}