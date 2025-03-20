import 'package:fury_core/src/excep/fury_exception.dart';

class DupTagRegistExcep extends FuryException {

  final String _tag;
  final Type _tagType;
  final Type _newType;

  DupTagRegistExcep(this._tag, this._tagType, this._newType);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('Duplicate registration for tag: ');
    buf.writeln(_tag);
    buf.write('\nThis tag is already registered for type: ');
    buf.writeln(_tagType);
    buf.write('\nBut you are now trying to register it for type: ');
    buf.writeln(_newType);
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}