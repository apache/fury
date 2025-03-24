import 'package:fury/src/codegen/analyze/impl/annotation/class_annotation_analyzer.dart';
import 'package:fury/src/codegen/analyze/impl/annotation/enum_annotation_analyzer.dart';
import 'package:fury/src/codegen/analyze/impl/annotation/key_annotation_analyzer.dart';
import 'package:fury/src/codegen/analyze/impl/constructor/constructor_analyzer.dart';
import 'package:fury/src/codegen/analyze/impl/field/access_info_analyzer.dart';
import 'package:fury/src/codegen/analyze/impl/field/field_analyzer_impl.dart';
import 'package:fury/src/codegen/analyze/impl/field/field_sortor.dart';
import 'package:fury/src/codegen/analyze/impl/field/fields_analyzer.dart';
import 'package:fury/src/codegen/analyze/impl/imports/imports_analyzer.dart';
import 'package:fury/src/codegen/analyze/impl/struct/class_analyzer_impl.dart';
import 'package:fury/src/codegen/analyze/impl/struct/custom_type_analyzer.dart';
import 'package:fury/src/codegen/analyze/impl/struct/enum_analyzer_impl.dart';
import 'package:fury/src/codegen/analyze/impl/type/type_analyzer_impl.dart';
import 'package:fury/src/codegen/analyze/impl/type/type_system_analyzer.dart';
import 'package:fury/src/codegen/analyze/interface/class_analyzer.dart';
import 'package:fury/src/codegen/analyze/interface/enum_analyzer.dart';
import 'package:fury/src/codegen/analyze/interface/field_analyzer.dart';
import 'package:fury/src/codegen/analyze/interface/type_analyzer.dart';
import 'package:fury/src/codegen/meta/gen_export.dart';

abstract class Analyzer {
  GenExport analyze(Object input);

  // Annotation analyzers
  static const ClassAnnotationAnalyzer classAnnotationAnalyzer = ClassAnnotationAnalyzer();
  static const KeyAnnotationAnalyzer keyAnnotationAnalyzer = KeyAnnotationAnalyzer();
  static const EnumAnnotationAnalyzer enumAnnotationAnalyzer = EnumAnnotationAnalyzer();

  // Enum analyzers
  static const EnumAnalyzer enumAnalyzer = EnumAnalyzerImpl();
  static const ClassAnalyzer classAnalyzer = ClassAnalyzerImpl();

  // Class analyzers
  static const ConstructorAnalyzer constructorAnalyzer = ConstructorAnalyzer();
  static const ImportsAnalyzer importsAnalyzer = ImportsAnalyzer();

  // Field analyzers
  static const AccessInfoAnalyzer accessInfoAnalyzer = AccessInfoAnalyzer();
  static const FieldsAnalyzer fieldsAnalyzer = FieldsAnalyzer();
  static const FieldAnalyzer fieldAnalyzer = FieldAnalyzerImpl();
  static const FieldsSorter fieldsSorter = FieldsSorter();

  // Type analyzers
  static final TypeAnalyzer typeAnalyzer = TypeAnalyzerImpl();
  static const TypeSystemAnalyzer typeSystemAnalyzer = TypeSystemAnalyzer();

  // Custom type analyzer
  static const CustomTypeAnalyzer customTypeAnalyzer = CustomTypeAnalyzer();
}
