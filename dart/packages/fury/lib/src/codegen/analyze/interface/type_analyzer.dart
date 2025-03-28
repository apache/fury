import 'package:fury/src/codegen/analyze/annotation/location_level_ensure.dart';
import 'package:fury/src/codegen/analyze/entity/analysis_res_wrap.dart';
import 'package:fury/src/codegen/const/location_level.dart';
import 'package:fury/src/codegen/entity/location_mark.dart';
import 'package:fury/src/codegen/meta/impl/type_spec_gen.dart';

abstract class TypeAnalyzer{

  TypeSpecGen getTypeImmutableAndTag(
    TypeDecision typeDecision,
    @LocationEnsure(LocationLevel.fieldLevel) LocationMark locationMark,
  );
}