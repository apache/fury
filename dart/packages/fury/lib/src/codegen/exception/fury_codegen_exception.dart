import 'package:fury/src/exception/fury_exception.dart';

abstract class FuryCodegenException extends FuryException {
  final String? _where;
  FuryCodegenException([this._where]);

  /// will generate warning and error location
  @override
  void giveExceptionMessage(StringBuffer buf) {
    buf.write(
'''[FURY]: Analysis error detected!
You need to make sure your codes don't contain any grammar error itself.
And review the error messages below, correct the issues, and then REGENERATE the code.
''');
    if (_where != null && _where.isNotEmpty) {
      buf.write('where: ');
      buf.write(_where);
      buf.write('\n');
    }
  }
}
