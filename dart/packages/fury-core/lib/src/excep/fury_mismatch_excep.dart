import 'package:fury_core/src/excep/fury_exception.dart';

class FuryMismatchException extends FuryException{
  final Object readValue;
  final Object expected;
  final String specification;

  FuryMismatchException(
    this.readValue,
    this.expected,
    this.specification,
  );

  @override
  void giveExcepMsg(StringBuffer buf) {
    super.giveExcepMsg(buf);
    buf.write('FuryMismatchException: ');
    buf.write(specification);
    buf.write('\nread value: ');
    buf.write(readValue);
    buf.write(' ,while expected: ');
    buf.write(expected);
    buf.write('\n');
  }

  @override
  String toString() {
    final buf = StringBuffer();
    giveExcepMsg(buf);
    return buf.toString();
  }
}