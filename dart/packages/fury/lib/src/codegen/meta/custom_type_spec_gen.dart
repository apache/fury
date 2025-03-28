import 'package:fury/src/codegen/meta/gen_export.dart';
import 'package:meta/meta.dart';

@immutable
abstract class CustomTypeSpecGen extends GenExport{
  final String name;
  final String importPath;

  const CustomTypeSpecGen(this.name, this.importPath);
}