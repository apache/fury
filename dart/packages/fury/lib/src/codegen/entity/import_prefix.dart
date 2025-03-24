import 'package:meta/meta.dart';

@immutable
class ImportPrefix{
  final int analyzeLibId;
  final String prefix;

  const ImportPrefix(this.analyzeLibId, this.prefix);
}