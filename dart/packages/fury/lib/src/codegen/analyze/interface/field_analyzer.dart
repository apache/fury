import 'package:analyzer/dart/element/element.dart';
import 'package:fury/src/codegen/analyze/annotation/location_level_ensure.dart';
import 'package:fury/src/codegen/const/location_level.dart';
import 'package:fury/src/codegen/entity/either.dart';
import 'package:fury/src/codegen/entity/location_mark.dart';
import 'package:fury/src/codegen/meta/impl/field_spec_immutable.dart';
import 'package:fury/src/codegen/meta/public_accessor_field.dart';

typedef FieldOverrideChecker = bool Function(String fieldName);

abstract class FieldAnalyzer{
  Either<FieldSpecImmutable, PublicAccessorField>? analyze(
    FieldElement element,
    FieldOverrideChecker fieldOverrideChecker,
    @LocationEnsure(LocationLevel.clsLevel) LocationMark locationMark,
  );
}