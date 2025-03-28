import 'package:fury/src/exception/fury_exception.dart';

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
  void giveExceptionMessage(StringBuffer buf) {
    super.giveExceptionMessage(buf);
    buf.write('FuryMismatchException: ');
    buf.write(specification);
    buf.write('\nread value: ');
    buf.write(readValue);
    buf.write(' ,while expected: ');
    buf.write(expected);
    buf.write('\n');
  }
}