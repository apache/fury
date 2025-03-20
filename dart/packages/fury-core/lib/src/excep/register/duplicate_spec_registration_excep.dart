import 'package:fury_core/src/excep/fury_exception.dart';

class DupTypeRegistExcep extends FuryException {

  final Type _forType;
  final String _newTag;

  DupTypeRegistExcep(this._forType, this._newTag);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('Duplicate registration for type: ');
    buf.writeln(_forType);
    buf.write('\nBut you try to register another tag: ');
    buf.writeln(_newTag);
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}