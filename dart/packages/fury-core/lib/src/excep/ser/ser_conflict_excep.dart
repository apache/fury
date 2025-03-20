import 'package:fury_core/src/excep/ser/serialization_excep.dart';

class SerConflictException extends SerializationException {
  final String _setting;
  final String _but;

  SerConflictException(this._setting, this._but, [super._where]);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('the setting: ');
    buf.writeln(_setting);
    buf.write('while: ');
    buf.writeln(_but);
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}