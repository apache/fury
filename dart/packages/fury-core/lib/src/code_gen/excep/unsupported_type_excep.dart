import 'package:fury_core/src/code_gen/excep/fury_gen_excep.dart';

class UnsupportedTypeException extends FuryGenExcep {

  final String clsLibPath;
  final String clsName;
  final String fieldName;

  final String typeScheme;
  final String typePath;
  final String typeName;

  UnsupportedTypeException(
    this.clsLibPath,
    this.clsName,
    this.fieldName,
    this.typeScheme,
    this.typePath,
    this.typeName,
  ): super('$clsLibPath@$clsName');

  /// will generate warning and error location
  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('Unsupported type: ');
    buf.write(typeScheme);
    buf.write(':');
    buf.write(typePath);
    buf.write('@');
    buf.write(typeName);
    buf.write('\n');
  }

  @override
  String toString() {
    StringBuffer buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}