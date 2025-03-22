import 'package:meta/meta.dart';

@immutable
class ImportPrefix{
  final int analyseLibId;
  final String prefix;

  const ImportPrefix(this.analyseLibId, this.prefix);
}