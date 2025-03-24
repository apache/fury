import 'package:fury/src/exception/fury_exception.dart';
import 'package:fury/src/const/obj_type.dart';

class UnsupportedTypeException extends FuryException{
  final ObjType _objType;

  UnsupportedTypeException(this._objType,);

  @override
  void giveExceptionMessage(StringBuffer buf) {
    super.giveExceptionMessage(buf);
    buf.write('unsupported type: ');
    buf.writeln(_objType);
  }
}