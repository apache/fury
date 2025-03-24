import 'package:analyzer/dart/element/element.dart';
import 'package:fury/src/codegen/meta/impl/class_spec_gen.dart';

abstract class ClassAnalyzer {
  ClassSpecGen analyze(ClassElement clsElement);
}