import 'package:analyzer/dart/element/element.dart';
import 'package:fury_core/src/code_gen/meta/impl/enum_spec_gen.dart';

abstract class EnumAnalyzer{
  EnumSpecGen analyze(EnumElement enumElement);
}