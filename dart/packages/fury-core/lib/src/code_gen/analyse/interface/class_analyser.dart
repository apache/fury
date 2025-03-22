import 'package:analyzer/dart/element/element.dart';
import 'package:fury_core/src/code_gen/meta/impl/class_spec_gen.dart';


abstract class ClassAnalyser {
  ClassSpecGen analyze(ClassElement clsElement);
}