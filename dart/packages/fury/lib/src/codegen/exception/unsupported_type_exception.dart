import 'package:fury/src/codegen/exception/fury_codegen_exception.dart';

class UnsupportedTypeException extends FuryCodegenException {

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
  void giveExceptionMessage(StringBuffer buf) {
    super.giveExceptionMessage(buf);
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
    giveExceptionMessage(buf);
    return buf.toString();
  }
}