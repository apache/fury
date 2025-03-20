import 'package:fury_core/src/excep/deser/deserialization_excep.dart';

class DeserInvalidParamException extends DeserializationException{
  final String _invalidParam;
  final String _validParams;

  DeserInvalidParamException(this._invalidParam, this._validParams, [super._where]);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('the invalid param: ');
    buf.writeln(_invalidParam);
    buf.write('while the valid params: ');
    buf.writeln(_validParams);
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}