import 'package:fury_core/src/excep/fury_exception.dart';

abstract class DeserializationException extends FuryException {
  final String? _where;

  DeserializationException([this._where]);

  @override
  void giveExcepMsg(StringBuffer buf) {
    if (_where != null) {
      buf.write('where: ');
      buf.writeln(_where);
    }
  }
}