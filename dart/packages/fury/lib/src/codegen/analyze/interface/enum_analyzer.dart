import 'package:analyzer/dart/element/element.dart';
import 'package:fury/src/codegen/meta/impl/enum_spec_gen.dart';

abstract class EnumAnalyzer{
  EnumSpecGen analyze(EnumElement enumElement);
}