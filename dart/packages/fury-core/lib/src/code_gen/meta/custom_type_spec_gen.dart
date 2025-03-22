import 'package:fury_core/src/code_gen/meta/gen_export.dart';
import 'package:meta/meta.dart';

@immutable
abstract class CustomTypeSpecGen extends GenExport{
  final String name;
  final String importPath;

  const CustomTypeSpecGen(this.name, this.importPath);
}