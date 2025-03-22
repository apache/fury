import 'package:fury_core/src/code_gen/excep/annotation/annotation_excep.dart';

class UnregisteredTypeException extends AnnotationExcep{
  final String _libPath;
  final String _clsName;

  final String _annotation;

  UnregisteredTypeException(this._libPath, this._clsName, this._annotation, [super._where]);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('Unregistered type: ');
    buf.write(_libPath);
    buf.write('@');
    buf.write(_clsName);
    buf.write('\nit should be registered with the annotation: ');
    buf.write(_annotation);
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}