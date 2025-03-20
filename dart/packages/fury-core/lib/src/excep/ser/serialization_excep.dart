import 'package:fury_core/src/excep/fury_exception.dart';

abstract class SerializationException extends FuryException {
  final String? _where;

  SerializationException([this._where]);

  @override
  void giveExcepMsg(StringBuffer buf) {
    if (_where != null) {
      buf.write('where: ');
      buf.writeln(_where);
    }
  }
}