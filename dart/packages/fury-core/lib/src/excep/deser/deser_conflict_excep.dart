import 'package:fury_core/src/excep/deser/deserialization_excep.dart';

class DeserConflictException extends DeserializationException{
  final String _readSetting;
  final String _nowFurySetting;

  DeserConflictException(this._readSetting, this._nowFurySetting, [super._where]);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('the fury instance setting: ');
    buf.writeln(_nowFurySetting);
    buf.write('while the read setting: ');
    buf.writeln(_readSetting);
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}