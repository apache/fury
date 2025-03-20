import 'package:fury_core/src/code_gen/excep/annotation/annotation_excep.dart';

class DuplicatedAnnotationExcep extends AnnotationExcep {
  final String _annotation;
  final String _displayName;

  // 类_className 有多个注解_annotation
  DuplicatedAnnotationExcep(this._annotation, this._displayName, [super._where]);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write(_displayName);
    buf.write(' has multiple ');
    buf.write(_annotation);
    buf.write(' annotations.');
    buf.write('\n');
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}