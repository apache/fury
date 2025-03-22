import 'package:fury_core/src/code_gen/analyse/impl/annotation/fury_enum_annotation_analyzer.dart';
import 'package:fury_core/src/code_gen/analyse/impl/field/access_info_analyzer.dart';
import 'package:fury_core/src/code_gen/analyse/impl/struct/fury_class_class_analyzer.dart';
import 'package:fury_core/src/code_gen/analyse/impl/type/type_system_analyzer.dart';
import 'package:fury_core/src/code_gen/analyse/impl/cons/constructor_analyzer.dart';
import 'package:fury_core/src/code_gen/analyse/impl/field/fields_analyzer.dart';
import 'package:fury_core/src/code_gen/analyse/impl/annotation/fury_class_annotation_analyzer.dart';
import 'package:fury_core/src/code_gen/analyse/impl/imports/imports_analyzer.dart';
import 'package:fury_core/src/code_gen/analyse/impl/struct/custom_type_analyzer.dart';
import 'package:fury_core/src/code_gen/analyse/interface/enum_analyzer.dart';
import 'package:fury_core/src/code_gen/analyse/interface/type_analyser.dart';
import 'package:fury_core/src/code_gen/meta/gen_export.dart';

import 'impl/annotation/fury_key_annotation_analyzer.dart';
import 'impl/field/field_sortor.dart';
import 'impl/struct/fury_enum_analyzer.dart';
import 'interface/field_analyser.dart';
import 'impl/field/fury_field_analyser.dart';
import 'impl/type/fury_type_analyser.dart';

abstract class Analyzer {
  GenExport analyse(Object input);

  /*-----------------------annotation--------------------------------------------------------------*/
  static const FuryClassAnnotationAnalyzer furyClassAnnotationAnalyzer = FuryClassAnnotationAnalyzer();
  static const FuryKeyAnnotationAnalyzer keyAnnotationAnalyzer = FuryKeyAnnotationAnalyzer();
  static const FuryEnumAnnotationAnalyzer furyEnumAnnotationAnalyzer = FuryEnumAnnotationAnalyzer();

  /*------------------------enum component--------------------------------------------------------*/
  static const EnumAnalyzer enumAnalyzer = FuryEnumAnalyzer();
  static const FuryClassClassAnalyser classAnalyzer = FuryClassClassAnalyser();

  /*------------------------class component--------------------------------------------------------*/
  static const ConstrcutorAnalyzer constructorAnalyzer = ConstrcutorAnalyzer();
  static const ImportsAnalyzer importsAnalyzer = ImportsAnalyzer();

  /*------------------------fields---------------------------------------------------------------*/
  static const AccessInfoAnalyzer accessInfoAnalyzer = AccessInfoAnalyzer();
  static const FieldsAnalyzer fieldsAnalyzer = FieldsAnalyzer();
  static const FieldAnalyser fieldAnalyser = FuryFieldAnalyser();
  static const FieldsSorter fieldsSorter = FieldsSorter();

  /*------------------------type---------------------------------------------------------------*/
  static final TypeAnalyser typeAnalyser = FuryTypeAnalyser();
  static const TypeSystemAnalyzer typeSystemAnalyzer = TypeSystemAnalyzer();


  static const CustomTypeAnalyzer customTypeAnalyzer = CustomTypeAnalyzer();

  // static const SpecSumAnalyser specSumAnalyser = FurySpecSumAnalyser();
}