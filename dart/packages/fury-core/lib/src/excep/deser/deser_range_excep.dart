import 'package:fury_core/src/excep/fury_exception.dart';

class DeserRangeExcep extends FuryException{
  final int index;
  final List<Object> candidates;

  DeserRangeExcep(this.index, this.candidates,);

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('the index $index is out of range, the candidates are: ');
    buf.write('[');
    buf.writeAll(candidates, ', ');
    buf.write(']\n');
    buf.write('This data may have inconsistencies on the other side');
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}