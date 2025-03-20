import 'package:fury_core/src/const/obj_type.dart';
import 'package:fury_core/src/excep/ser/serialization_excep.dart';

class SerRangeExcep extends SerializationException {
  final ObjType _specified;
  final num _yourValue;

  SerRangeExcep(this._specified, this._yourValue, [super._where]);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('the specified type: ');
    buf.writeln(_specified);
    buf.write('while your value: ');
    buf.writeln(_yourValue);
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}