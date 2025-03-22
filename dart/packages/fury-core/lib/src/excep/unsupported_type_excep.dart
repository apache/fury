import 'package:fury_core/fury_core.dart';
import 'package:fury_core/src/excep/fury_exception.dart';

class UnsupportedTypeExcep extends FuryException{
  final ObjType _objType;

  UnsupportedTypeExcep(this._objType,);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('unsupported type: ');
    buf.writeln(_objType);
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}