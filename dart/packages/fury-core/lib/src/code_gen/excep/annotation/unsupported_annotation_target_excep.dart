import 'package:fury_core/src/code_gen/excep/annotation/annotation_excep.dart';
import 'package:meta/meta_meta.dart';

class UnsupportedAnnotationTargetExcep extends AnnotationExcep {
  // final List<AnnotationTarget> _should;
  final String _annotation;
  final String _theTarget;
  final List<TargetKind> _supported;

  UnsupportedAnnotationTargetExcep(this._annotation, this._theTarget, this._supported, [super._where]);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('Unsupported target for annotation: ');
    buf.writeln(_annotation);
    buf.write('Target: ');
    buf.writeln(_theTarget);
    buf.write('Supported targets: ');
    buf.writeAll(_supported, ', ');
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}