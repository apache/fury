import 'package:fury_core/src/excep/fury_exception.dart';

class UnregisteredTypeExcep extends FuryException {
  final Type _type;

  UnregisteredTypeExcep(this._type);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('Unregistered type: ');
    buf.writeln(_type);
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}