import 'deserialization_excep.dart';

class DeserUnsupportedFeatureExcep extends DeserializationException{

  final Object _read;
  final List<Object> _supported;
  final String _whatFeature;

  DeserUnsupportedFeatureExcep(this._read, this._supported, this._whatFeature, [super._where]);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('unsupported ');
    buf.write(_whatFeature);
    buf.write(' for type: ');
    buf.writeln(_read);
    buf.write('supported: ');
    buf.writeAll(_supported, ', ');
    buf.write('\n');
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}