import 'annotation_excep.dart';

class ConflictAnnotationException extends AnnotationExcep {
  final String _targetAnnotation;
  final String _conflictAnnotation;

  ConflictAnnotationException(
    this._targetAnnotation,
    this._conflictAnnotation,
    [super._where]
  );

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('The annotation $_targetAnnotation conflicts with $_conflictAnnotation.');
    buf.write('\n');
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}