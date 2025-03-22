import 'package:fury_core/src/excep/fury_exception.dart';

class UnregisteredExcep extends FuryException {
  final String _tag;

  UnregisteredExcep(this._tag);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('Unregistered tag: ');
    buf.writeln(_tag);
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}